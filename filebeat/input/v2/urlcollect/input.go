package urlcollect

import (
	"context"
	"net/url"

	v2 "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/urso/sderr"
)

type urlsInput struct {
	pluginName string
	urls       []*url.URL
	manager    *InputManager
	input      Input
}

func (m *urlsInput) Run(ctx v2.Context, pc beat.PipelineConnector) error {
	workers, err := m.createWorkers(ctx, pc)
	if err != nil {
		return err
	}

	var group group
	for i, w := range workers {
		group.Go(func() error {
			defer w.client.Close()
			err := m.manager.run(ctx, m.pluginName, m.urls[i], w)
			if err == context.Canceled {
				err = nil
			}
			return err
		})
	}

	if errs := group.Wait(); len(errs) > 0 {
		return sderr.WrapAll(errs, "url collector %{id} failed", ctx.ID)
	}
	return nil
}

func (m *urlsInput) createWorkers(ctx v2.Context, pc beat.PipelineConnector) ([]managedInput, error) {
	var workers []managedInput
	for i, url := range m.urls {
		client, err := pc.ConnectWith(beat.ClientConfig{
			CloseRef: ctx.Cancelation,
			Processing: beat.ProcessingConfig{
				DynamicFields: ctx.Metadata,
			},
		})
		if err != nil {
			for _, w := range workers {
				w.client.Close()
			}
			return nil, sderr.Wrap(err, "failed to setup client for %{url}", url)
		}

		workers[i] = managedInput{
			client: client,
			run:    m.input.Run,
		}
	}

	return workers, nil
}

// Test runs the inputs' Test function for each configured base URL.
func (m *urlsInput) Test(ctx v2.TestContext) error {
	if m.input.Test == nil || len(m.urls) == 0 {
		return nil
	}

	var group group
	for _, url := range m.urls {
		group.Go(func() error { return m.input.Test(ctx, url) })
	}

	if errs := group.Wait(); len(errs) > 0 {
		sderr.WrapAll(errs, "input test for failed")
	}
	return nil
}
