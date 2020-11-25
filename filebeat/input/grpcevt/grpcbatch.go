package grpcevt

import (
	"context"
	"io"
	"net"
	"time"

	input "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/rpcdef/datareq"
	"github.com/elastic/go-concert/ctxtool"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

type grpcInput struct {
	address      string
	reqKeepalive time.Duration
}

type grpcHandler struct {
	datareq.UnimplementedDataServiceServer

	client       beat.Client
	logger       *logp.Logger
	reqKeepalive time.Duration
}

func newGRPCInput(settings settings) (*grpcInput, error) {
	return &grpcInput{
		address:      settings.Address,
		reqKeepalive: settings.Keepalive,
	}, nil
}

func (g *grpcInput) Name() string                   { return grpcPluginName }
func (g *grpcInput) Test(_ input.TestContext) error { return nil }

func (g *grpcInput) Run(ctx input.Context, pipeline beat.Pipeline) error {
	client, err := pipeline.ConnectWith(beat.ClientConfig{
		ACKHandler: newPipelineACKer(),
	})
	if err != nil {
		return err
	}
	defer client.Close()

	grpcHandler, err := newGrpcHandler(ctx.Logger, client, g.reqKeepalive)
	if err != nil {
		return err
	}

	var opts []grpc.ServerOption // TODO
	grpcServer := grpc.NewServer(opts...)
	datareq.RegisterDataServiceServer(grpcServer, grpcHandler)

	listener, err := net.Listen("tcp", g.address)
	if err != nil {
		return err
	}
	defer listener.Close()

	_, cancel := ctxtool.WithFunc(ctxtool.FromCanceller(ctx.Cancelation), func() {
		grpcServer.Stop()
	})
	defer cancel()

	return grpcServer.Serve(listener)
}

func newGrpcHandler(logger *logp.Logger, client beat.Client, keepalive time.Duration) (*grpcHandler, error) {
	return &grpcHandler{logger: logger, client: client, reqKeepalive: keepalive}, nil
}

func (g *grpcHandler) PublishBulk(req *datareq.EventPublishRequest, stream datareq.DataService_PublishBulkServer) error {
	ackHandler := newBatchACKHandler()
	g.logger.Debugf("GRPC Bulk request received: %v", len(req.Events))

	go func() {
		for _, rpcEvent := range req.Events {
			// stop publishing in case we detected an IO error in the response stream
			select {
			case <-ackHandler.breaker:
				return
			default:
			}

			event := decodeEvent(rpcEvent)
			event.Private = ackHandler
			g.client.Publish(event)
		}
	}()

	ticker := time.NewTicker(g.reqKeepalive)
	defer ticker.Stop()

	total := len(req.Events)
	var acked int
	for {
		select {
		case <-ticker.C:
			err := stream.Send(&datareq.EventPublishResponse{Acked: uint64(acked)})
			if err != nil {
				close(ackHandler.breaker)
				g.logger.Errorf("Failed to send bulk activity signal to client: %v", err)
				return err
			}
		case n := <-ackHandler.ch:
			acked += n
		}

		if acked == total {
			break
		}
	}

	err := stream.Send(&datareq.EventPublishResponse{Acked: uint64(acked)})
	if err != nil {
		g.logger.Errorf("Failed to send final bulk ACK to client: %v", err)
	}

	return nil
}

func (g *grpcHandler) PublishStream(stream datareq.DataService_PublishStreamServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	ackHandler := newBatchACKHandler()

	chEvents := make(chan int, 100)
	grp, ctx := errgroup.WithContext(ctx)

	// event publishing loop
	grp.Go(func() error {
		for ctx.Err() == nil {
			rpcEvent, err := stream.Recv()
			if err == io.EOF {
				return nil // client did close send channel.
			}
			if err != nil {
				cancel()
				close(ackHandler.breaker)
				return err // connection/network problem. Let's shutdown with error
			}

			if ctx.Err() != nil {
				break
			}

			event := decodeEvent(rpcEvent)
			event.Private = ackHandler
			chEvents <- 1
			g.client.Publish(event)
		}
		return nil
	})

	// ack loop
	grp.Go(func() error {
		var activeEvents int

		var ticker *time.Ticker
		var tickerCh <-chan time.Time

		defer func() {
			if ticker != nil {
				ticker.Stop()
			}
		}()

		for ctx.Err() == nil {
			var err error

			select {
			case <-ctx.Done():
				return nil

			case n := <-chEvents:
				if activeEvents == 0 && n > 0 {
					ticker = time.NewTicker(g.reqKeepalive)
					tickerCh = ticker.C
				}
				activeEvents += n

			case <-tickerCh:
				err = stream.Send(&datareq.EventPublishResponse{Acked: 0})

			case n := <-ackHandler.ch:
				activeEvents -= n
				if activeEvents == 0 {
					ticker.Stop()
					ticker = nil
					tickerCh = nil
				}

				err = stream.Send(&datareq.EventPublishResponse{Acked: uint64(n)})
			}

			if err != nil { // receiving io.EOF means that the client did close the connection. In that case we must stop accepting any new events.
				cancel()
				close(ackHandler.breaker)
				return err // connection/network problem. Let's shutdown with error
			}
		}
		return nil
	})

	return grp.Wait()
}
