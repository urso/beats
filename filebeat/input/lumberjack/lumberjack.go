package lumberjack

import (
	"time"

	input "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/go-concert/ctxtool"
	"github.com/elastic/go-concert/unison"
	"github.com/elastic/go-lumber/lj"
	v2 "github.com/elastic/go-lumber/server/v2"
)

type lumberjackInput struct {
	address   string
	worker    int
	keepalive time.Duration
}

type worker struct {
	client    beat.Client
	workqueue <-chan *lj.Batch
}

func newLumerjackInput(settings settings) (*lumberjackInput, error) {
	return &lumberjackInput{
		address:   settings.Address,
		worker:    settings.Worker,
		keepalive: settings.Keepalive,
	}, nil
}

// Name reports the input name.
func (l *lumberjackInput) Name() string {
	return pluginName
}

// Test checks the configuaration and runs additional checks if the Input can
// actually collect data for the given configuration (e.g. check if host/port or files are
// accessible).
func (l *lumberjackInput) Test(_ input.TestContext) error {
	return nil
}

// Run starts the data collection. Run must return an error only if the
// error is fatal making it impossible for the input to recover.
func (l *lumberjackInput) Run(ctx input.Context, pipeline beat.PipelineConnector) error {
	var autoCancel ctxtool.AutoCancel
	defer autoCancel.Cancel()

	batchWorkQueue := make(chan *lj.Batch, l.worker)

	grp := unison.TaskGroup{
		StopOnError: func(_ error) bool { return true },
	}
	stopCtx := autoCancel.With(ctxtool.WithFunc(ctxtool.FromCanceller(ctx.Cancelation), func() {
		grp.Stop()
	}))

	for i := 0; i < l.worker; i++ {
		client, err := pipeline.ConnectWith(beat.ClientConfig{
			ACKHandler: newPipelineACKer(),
		})
		if err != nil {
			return err
		}
		defer client.Close()

		worker := &worker{client: client, workqueue: batchWorkQueue}
		grp.Go(worker.Run)
	}

	decoder := &jsonDecoder{
		logger: ctx.Logger,
	}

	server, err := v2.ListenAndServe(
		l.address,
		v2.Channel(batchWorkQueue),
		v2.Keepalive(l.keepalive),
		v2.JSONDecoder(decoder.Decode),
	)
	if err != nil {
		return err
	}

	stopCtx = autoCancel.With(ctxtool.WithFunc(stopCtx, func() {
		server.Close()
	}))

	<-stopCtx.Done()
	return nil
}

func (w *worker) Run(canceler unison.Canceler) error {
	for canceler.Err() == nil {
		var batch *lj.Batch
		select {
		case <-canceler.Done():
			return canceler.Err()
		case b := <-w.workqueue:
			batch = b
		}

		if batch == nil {
			continue
		}

		ackHandler := newBatchACKHandler(batch)
		for _, batchEvent := range batch.Events {
			event, ok := batchEvent.(beat.Event)
			if !ok {
				ackHandler.ack(1) // ignore decoding errors
				// TODO: log error
				continue
			}

			event.Private = ackHandler
			w.client.Publish(event)
		}
	}

	return nil
}
