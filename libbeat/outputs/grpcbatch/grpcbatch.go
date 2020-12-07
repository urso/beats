package grpcbatch

import (
	"context"
	"fmt"
	"io"

	"google.golang.org/grpc"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/codec/cbor"
	"github.com/elastic/beats/v7/libbeat/publisher"
	"github.com/elastic/beats/v7/libbeat/rpcdef/datareq"
)

type grpcbatchClient struct {
	log *logp.Logger

	conn   *grpc.ClientConn
	sender batchSender

	host     string
	settings settings
	stats    outputs.Observer
}

type batchSender interface {
	send(context.Context, []publisher.Event) (responseStream, int, error)
}

type protobufSender struct {
	log    *logp.Logger
	client datareq.DataServiceClient
}

type cborSender struct {
	log     *logp.Logger
	client  datareq.DataServiceClient
	encoder *cbor.Encoder
}

var eventSeq uint64

func configureBatch(
	_ outputs.IndexManager,
	_ beat.Info,
	stats outputs.Observer,
	cfg *common.Config,
) (outputs.Group, error) {
	settings, err := readSettings(cfg)
	if err != nil {
		return outputs.Fail(err)
	}

	hosts, err := outputs.ReadHostList(cfg)
	if err != nil {
		return outputs.Fail(err)
	}

	log := logp.NewLogger("grpcbatch")
	clients := make([]outputs.NetworkClient, len(hosts))
	for i, host := range hosts {
		client, err := newBatchClient(log, stats, host, settings)
		if err != nil {
			return outputs.Fail(err)
		}

		clients[i] = outputs.WithBackoff(client, settings.Backoff.Init, settings.Backoff.Max)
	}

	loadbalance := len(hosts) > 1
	return outputs.SuccessNet(loadbalance, settings.BulkMaxSize, 1, clients)
}

func newBatchClient(log *logp.Logger, stats outputs.Observer, host string, settings settings) (*grpcbatchClient, error) {
	codec := settings.Codec
	if codec == "" {
		codec = "protobuf"
	}
	switch codec {
	case "protobuf", "cbor":
	default:
		return nil, fmt.Errorf("Unsupported codec type: %v", codec)
	}

	dialOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}

	if settings.Compression != 0 {
		compressor, err := grpc.NewGZIPCompressorWithLevel(settings.Compression)
		if err != nil {
			return nil, err
		}

		dialOpts = append(dialOpts, grpc.WithCompressor(compressor))
	}

	// connections are handled in background for us. Let's create one upfront
	conn, err := grpc.Dial(host, dialOpts...)
	if err != nil {
		return nil, err
	}

	log.Debugf("New GRPC Batch client with codec=%v", codec)

	grpcClient := datareq.NewDataServiceClient(conn)

	var sender batchSender
	switch codec {
	case "protobuf":
		sender = &protobufSender{
			log:    log,
			client: grpcClient,
		}
	case "cbor":
		sender = &cborSender{
			log:     log,
			client:  grpcClient,
			encoder: cbor.New("8.0.0"),
		}
	default:
		return nil, fmt.Errorf("Codec not supported: %v", codec)
	}

	return &grpcbatchClient{
		conn:     conn,
		log:      log,
		stats:    stats,
		host:     host,
		settings: settings,
		sender:   sender,
	}, nil
}

// Connect establishes a connection to the clients sink.
// The connection attempt shall report an error if no connection could been
// established within the given time interval. A timeout value of 0 == wait
// forever.
func (g *grpcbatchClient) Connect() error {
	return nil
}

func (g *grpcbatchClient) Close() error {
	// TODO: close underlying connection object
	return nil
}

func (g *grpcbatchClient) String() string {
	return "grcpbatch"
}

type responseStream interface {
	Recv() (*datareq.EventPublishResponse, error)
}

func (g *grpcbatchClient) Publish(ctx context.Context, batch publisher.Batch) error {
	events := batch.Events()
	g.stats.NewBatch(len(events))

	respStream, published, err := g.sender.send(ctx, events)
	dropped := len(events) - published
	if err != nil {
		// TODO: check status code
		g.stats.Failed(len(events))
		batch.Retry()
		g.log.Errorf("Failed to publish events: %v", err)
		return err
	}

	if published == 0 {
		batch.ACK()
		return nil
	}

	var acked uint64
	for {
		resp, err := respStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			g.stats.Failed(len(events))
			batch.Retry()
			g.log.Errorf("Failed to publish events: waiting for ACK: %v", err)
			return err
		}

		acked = resp.Acked
	}

	if acked < uint64(published) {
		g.log.With("published", published, "acked", acked).Errorf("Connection was closed before all events have been ACKed.")
		g.stats.Failed(len(events))
		batch.Retry()
		return io.EOF
	}

	g.stats.Dropped(dropped)
	g.stats.Acked(published)
	batch.ACK()

	return nil
}

func (s *protobufSender) send(ctx context.Context, events []publisher.Event) (responseStream, int, error) {
	var dropped int
	req := datareq.EventPublishRequest{Events: make([]*datareq.Event, 0, len(events))}
	for i := range events {
		rpcEvent, err := encodeEvent(&events[i].Content)
		if err != nil {
			dropped++
			s.log.Errorf("Failed to encode event, dropping: %v", err)
			continue
		}

		req.Events = append(req.Events, rpcEvent)
	}

	published := len(req.Events)
	if published == 0 {
		return nil, 0, nil
	}

	respStream, err := s.client.PublishBulk(ctx, &req)
	return respStream, published, err
}

func (s *cborSender) send(ctx context.Context, events []publisher.Event) (responseStream, int, error) {
	req := datareq.RawEventBatch{Events: make([][]byte, 0, len(events))}
	for i := range events {
		rpcEvent, err := s.encoder.Encode("", &events[i].Content)
		if err != nil {
			s.log.Errorf("Failed to encode event, dropping: %v", err)
			continue
		}

		tmp := make([]byte, len(rpcEvent))
		copy(tmp, rpcEvent)

		req.Events = append(req.Events, tmp)
		eventSeq++
	}

	published := len(req.Events)
	if published == 0 {
		return nil, 0, nil
	}

	respStream, err := s.client.PublishBulkCBOR(ctx, &req)
	return respStream, published, err
}
