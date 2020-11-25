package grpcbatch

import (
	"context"
	"io"

	"google.golang.org/grpc"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/publisher"
	"github.com/elastic/beats/v7/libbeat/rpcdef/datareq"
)

type grpcbatchClient struct {
	log *logp.Logger

	conn   *grpc.ClientConn
	client datareq.DataServiceClient

	host     string
	settings settings
	stats    outputs.Observer
}

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
	// connections are handled in background for us. Let's create one upfront
	conn, err := grpc.Dial(host, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	grpcClient := datareq.NewDataServiceClient(conn)

	return &grpcbatchClient{
		client:   grpcClient,
		conn:     conn,
		log:      log,
		stats:    stats,
		host:     host,
		settings: settings,
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

func (g *grpcbatchClient) Publish(ctx context.Context, batch publisher.Batch) error {
	events := batch.Events()
	g.stats.NewBatch(len(events))

	var dropped int
	req := datareq.EventPublishRequest{Events: make([]*datareq.Event, 0, len(events))}
	for i := range events {
		rpcEvent, err := encodeEvent(&events[i].Content)
		if err != nil {
			dropped++
			g.log.Errorf("Failed to encode event, dropping: %v", err)
			continue
		}

		g.log.Debugf("Add RPC event: %v", rpcEvent)
		req.Events = append(req.Events, rpcEvent)
	}

	published := len(req.Events)
	respStream, err := g.client.PublishBulk(ctx, &req)
	if err != nil {
		// TODO: check status code
		g.stats.Failed(len(events))
		batch.Retry()
		g.log.Errorf("Failed to publish events: %v", err)
		return err
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
