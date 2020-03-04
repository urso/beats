package urlcollect

import (
	"fmt"
	"sync"

	v2 "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/filebeat/input/v2/statestore"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/registry"
)

// InputManager controls the execution of active inputs.
type InputManager struct {
	Logger   *logp.Logger
	Registry *registry.Registry

	initOnce       sync.Once
	storeConnector *statestore.Connector
}

type managedInput struct {
	manager *InputManager
	run     func() error
}

func (m *InputManager) init() {
	m.initOnce.Do(func() {
		if m.Registry == nil {
			panic(fmt.Errorf("no registry"))
		}
		if m.Logger == nil {
			panic(fmt.Errorf("no logger"))
		}

		if m.storeConnector == nil {
			m.storeConnector = statestore.NewConnector(m.Registry)
		}
	})
}

func (m *managedInput) Run(ctx v2.Context, pc beat.PipelineConnector) error {
	conn, err := pc.ConnectWith(beat.ClientConfig{
		// apply commnont input type specific settings
	})
	if err != nil {
		return err
	}

	panic("TODO")
}
