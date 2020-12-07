package grpcbatch

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/common/atomic"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/codec/cbor"
	"github.com/elastic/beats/v7/libbeat/publisher"
	"github.com/elastic/beats/v7/libbeat/rpcdef/datareq"
	"github.com/elastic/go-concert/unison"
	"google.golang.org/grpc"
)

type grpcStreamClient struct {
	log *logp.Logger

	conn   *grpc.ClientConn
	client datareq.DataServiceClient

	// active stream details
	mu           sync.Mutex
	streamClient *asyncClient

	host     string
	settings settings
	stats    outputs.Observer

	useCBOR bool
}

type asyncClient struct {
	log    *logp.Logger
	ctx    context.Context
	cancel context.CancelFunc

	stream     datareq.DataService_PublishStreamClient
	streamCBOR datareq.DataService_PublishStreamCBORClient
	encoder    *cbor.Encoder

	wg            unison.SafeWaitGroup
	finishedClose chan struct{}

	chSchedule chan *batchRef
	chACKs     chan int
	chFail     chan error
}

func configureStream(
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
		client, err := newStreamClient(log, stats, host, settings)
		if err != nil {
			return outputs.Fail(err)
		}

		clients[i] = outputs.WithBackoff(client, settings.Backoff.Init, settings.Backoff.Max)
	}

	loadbalance := len(hosts) > 1
	return outputs.SuccessNet(loadbalance, 0, 1, clients)
}

func newStreamClient(log *logp.Logger, stats outputs.Observer, host string, settings settings) (*grpcStreamClient, error) {
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

	grpcClient := datareq.NewDataServiceClient(conn)

	return &grpcStreamClient{
		client:   grpcClient,
		conn:     conn,
		log:      log,
		stats:    stats,
		host:     host,
		settings: settings,
		useCBOR:  codec == "cbor",
	}, nil
}

func (g *grpcStreamClient) String() string { return "grcpbatch" }

func (g *grpcStreamClient) Connect() error {
	streamCtx, cancel := context.WithCancel(context.Background())

	var streamCBOR datareq.DataService_PublishStreamCBORClient
	var stream datareq.DataService_PublishStreamClient

	var err error
	if g.useCBOR {
		streamCBOR, err = g.client.PublishStreamCBOR(streamCtx)
	} else {
		stream, err = g.client.PublishStream(streamCtx)
	}
	if err != nil {
		defer cancel()
		return err
	}

	client := &asyncClient{
		log:        g.log,
		ctx:        streamCtx,
		cancel:     cancel,
		stream:     stream,
		streamCBOR: streamCBOR,
		encoder:    cbor.New("8.0.0"),

		finishedClose: make(chan struct{}),

		chSchedule: make(chan *batchRef, 10),
		chACKs:     make(chan int, 10),
		chFail:     make(chan error, 1),
	}
	if g.useCBOR {
		go client.recvLoop(streamCBOR)
	} else {
		go client.recvLoop(stream)
	}
	go client.ackLoop()

	// TODO: set and check headers (handshake like)

	g.mu.Lock()
	defer g.mu.Unlock()
	g.streamClient = client

	return nil
}

func (g *grpcStreamClient) Close() error {
	g.mu.Lock()
	client := g.streamClient
	g.streamClient = nil
	g.mu.Unlock()

	if client == nil {
		return nil
	}
	return client.Close()
}

func (g *grpcStreamClient) Publish(ctx context.Context, batch publisher.Batch) error {
	g.mu.Lock()
	client := g.streamClient
	g.mu.Unlock()

	events := batch.Events()
	g.stats.NewBatch(len(events))

	if client == nil || client.ctx.Err() != nil {
		batch.Retry()
		g.stats.Failed(len(events))
		return errors.New("client closed")
	}

	if err := client.wg.Add(1); err != nil {
		batch.Retry()
		g.stats.Failed(len(events))
		return errors.New("client closed")
	}
	defer client.wg.Done()

	batchSize := len(events)

	batchRef := &batchRef{
		logger: g.log,
		stats:  g.stats,
		count:  atomic.MakeUint32(1 + uint32(batchSize)),
		batch:  batch,
		err:    nil,
	}
	defer batchRef.dec()

	var processed int
	for _, event := range batch.Events() {
		err := client.Send(event, batchRef)
		processed++
		if err != nil {
			break
		}
	}
	for range events[processed:] { // clean up for events that are not going to be send anymore
		batchRef.dec()
	}

	return nil
}

func (client *asyncClient) Close() error {
	client.cancel()
	client.wg.Wait()
	if client.finishedClose != nil {
		close(client.finishedClose)
		client.finishedClose = nil
	}
	return nil
}

func (client *asyncClient) Send(event publisher.Event, ref *batchRef) error {
	if err := client.ctx.Err(); err != nil {
		ref.fail(err)
		return err
	}

	client.chSchedule <- ref
	if client.stream != nil {
		rpcEvent, err := encodeEvent(&event.Content)
		if err != nil {
			client.log.Debugf("Dropping event due to encoding error: %v", err)
			ref.dec()
			ref.stats.Dropped(1)
			return nil
		}

		err = client.stream.Send(rpcEvent)
		if err != nil {
			client.log.Debug("failed to publish event: %v", err)
			ref.fail(err)
			return err
		}
	} else {
		rpcEvent, err := encodeEventCBOR(client.encoder, &event.Content)
		if err != nil {
			client.log.Debugf("Dropping event due to encoding error: %v", err)
			ref.dec()
			return nil
		}

		err = client.streamCBOR.Send(rpcEvent)
		if err != nil {
			client.log.Debug("failed to publish event: %v", err)
			ref.fail(err)
			return err
		}
	}

	return nil
}

func (client *asyncClient) ackLoop() {
	var err error
	var pending []*batchRef

	defer func() {
		if err == nil {
			err = errors.New("client shutting down")
		}
		for _, ref := range pending {
			ref.fail(err)
		}
	}()

	for {
		select {
		case <-client.finishedClose:
			return

		case ref := <-client.chSchedule:
			if err == nil {
				pending = append(pending, ref)
			} else {
				ref.fail(err) // connection is already broken => each event is marked as failed
			}

		case fail := <-client.chFail:
			err = fail
			for _, ref := range pending {
				ref.fail(err)
			}
			pending = nil

		case n := <-client.chACKs:
			if err == nil {
				acked := pending[:n]
				pending = pending[n:]
				for _, ref := range acked {
					ref.dec()
				}
			}
		}
	}
}

func (client *asyncClient) recvLoop(stream interface {
	Recv() (*datareq.EventPublishResponse, error)
}) {
	for client.ctx.Err() == nil {
		resp, err := stream.Recv()
		if err != nil {
			client.log.Errorf("GRPC stream broken. Closing connection. Reason: %v", err)
			client.chFail <- err
			client.Close()
			return
		}

		client.chACKs <- int(resp.Acked)
	}
}
