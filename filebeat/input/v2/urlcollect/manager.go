package urlcollect

import (
	"net/url"

	v2 "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/filebeat/input/v2/statestore"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/registry"
	"github.com/elastic/go-concert/ctxtool"
	"golang.org/x/sync/errgroup"
)

// InputManager controls the execution of active inputs.
type InputManager struct {
	log      *logp.Logger
	registry *registry.Registry

	storeConnector *statestore.Connector
}

type InputFunc func() error

type TestFunc func(v2.TestContext, *url.URL) error

type managedInput struct {
	urls    []*url.URL
	manager *InputManager
	input   Input
}

type urlInputState struct {
	url         *url.URL
	registryKey string
	err         error
}

func NewInputManager(log *logp.Logger, registry *registry.Registry) *InputManager {
	required(registry != nil, "no registry")
	required(log != nil, "no logger")

	log = log.Named("urlcollect")
	return &InputManager{
		log:            log,
		registry:       registry,
		storeConnector: statestore.NewConnector(log, registry),
	}
}

func (m *managedInput) Run(ctx v2.Context, pc beat.PipelineConnector) error {
	panic("TODO")
}

// Test runs the inputs' Test function for each configured base URL.
func (m *managedInput) Test(ctx v2.TestContext) error {
	if m.input.Test == nil || len(m.urls) == 0 {
		return nil
	}

	grp, grpContext := errgroup.WithContext(ctxtool.FromCanceller(ctx.Cancelation))
	ctx.Cancelation = grpContext
	for _, url := range m.urls {
		grp.Go(func() error {
			return m.input.Test(ctx, url)
		})
	}
	return grp.Wait()
}
