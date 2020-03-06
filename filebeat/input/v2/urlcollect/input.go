package urlcollect

import (
	"net/url"

	v2 "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/urso/sderr"
)

type managedInput struct {
	pluginName string
	urls       []*url.URL
	manager    *InputManager
	input      Input
}

type urlInput struct {
	cursorKey string
	url       *url.URL
	manager   *InputManager
	client    beat.Client
	runFunc   InputFunc
}

func (m *managedInput) Run(ctx v2.Context, pc beat.PipelineConnector) error {
	workers, err := m.createWorkers(pc)
	if err != nil {
		return err
	}

	var group group
	for _, w := range workers {
		group.Go(func() error { return w.run(ctx) })
	}

	if errs := group.Wait(); len(errs) > 0 {
		return sderr.WrapAll(errs, "url collector %{id} failed", ctx.ID)
	}
	return nil
}

// Test runs the inputs' Test function for each configured base URL.
func (m *managedInput) Test(ctx v2.TestContext) error {
	if m.input.Test == nil || len(m.urls) == 0 {
		return nil
	}

	var group group
	for _, url := range m.urls {
		group.Go(func() error { return m.input.Test(ctx, url) })
	}

	if errs := group.Wait(); len(errs) > 0 {
		sderr.WrapAll(errs, "input test for '%{id}' failed", ctx.ID)
	}
	return nil
}

func (m *managedInput) createWorkers(pc beat.PipelineConnector) ([]urlInput, error) {
	keyPrefix := "urlcollect::" + m.pluginName

	workers := make([]urlInput, len(m.urls))
	for i, url := range m.urls {
		client, err := pc.ConnectWith(beat.ClientConfig{})
		if err != nil {
			return nil, sderr.Wrap(err, "failed to setup client for %{url}", url)
		}

		workers[i] = urlInput{
			cursorKey: keyPrefix + "::" + url.String(),
			url:       url,
			client:    client,
			manager:   m.manager,
			runFunc:   m.input.Run,
		}
	}

	return workers, nil
}

func (input *urlInput) run(ctx v2.Context) (err error) {
	input.manager.withCursor(input.cursorKey, func(c Cursor) {

	})
	if err != nil {
		err = sderr.Wrap(err, "worker for %{url} failed", input.url)
	}
	return err
}