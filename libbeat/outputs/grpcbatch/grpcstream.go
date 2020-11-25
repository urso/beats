package grpcbatch

import (
	"context"
	"errors"
	"sync"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/common/atomic"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/outputs"
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
}

type asyncClient struct {
	log    *logp.Logger
	ctx    context.Context
	cancel context.CancelFunc
	stream datareq.DataService_PublishStreamClient

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
	// connections are handled in background for us. Let's create one upfront
	conn, err := grpc.Dial(host, grpc.WithInsecure())
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
	}, nil
}

func (g *grpcStreamClient) String() string { return "grcpbatch" }

func (g *grpcStreamClient) Connect() error {
	streamCtx, cancel := context.WithCancel(context.Background())
	stream, err := g.client.PublishStream(streamCtx)
	if err != nil {
		defer cancel()
		return err
	}

	client := &asyncClient{
		log:    g.log,
		ctx:    streamCtx,
		cancel: cancel,
		stream: stream,

		finishedClose: make(chan struct{}),

		chSchedule: make(chan *batchRef, 10),
		chACKs:     make(chan int, 10),
		chFail:     make(chan error, 1),
	}
	go client.recvLoop()
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

	return client.Close()
}

func (g *grpcStreamClient) Publish(ctx context.Context, batch publisher.Batch) error {
	g.mu.Lock()
	client := g.streamClient
	g.mu.Unlock()

	if client == nil || client.ctx.Err() != nil {
		batch.Retry()
		return errors.New("client closed")
	}

	if err := client.wg.Add(1); err != nil {
		return errors.New("client closed")
	}
	defer client.wg.Done()

	events := batch.Events()
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
	close(client.finishedClose)
	return nil
}

func (client *asyncClient) Send(event publisher.Event, ref *batchRef) error {
	rpcEvent, err := encodeEvent(&event.Content)
	if err != nil {
		client.log.Debugf("Dropping event due to encoding error: %v", err)
		ref.dec()
		return nil
	}

	if client.ctx.Err() != nil {
		return client.ctx.Err()
	}

	err = client.stream.Send(rpcEvent)
	if err != nil {
		ref.fail(err)
		return err
	}

	client.chSchedule <- ref
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

func (client *asyncClient) recvLoop() {
	for client.ctx.Err() == nil {
		resp, err := client.stream.Recv()
		if err != nil {
			client.log.Errorf("GRPC stream broken. Closing connection. Reason: %v", err)
			client.chFail <- err
			client.Close()
			return
		}

		client.chACKs <- int(resp.Acked)
	}
}
