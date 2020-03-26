package v2

import (
	"fmt"
	"runtime/debug"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
)

type StatelessInputManager struct {
	Configure func(*common.Config) (StatelessInput, error)
}

type StatelessInput interface {
	Test(TestContext) error
	Run(ctx Context, publish func(beat.Event)) error
}

type statelessInputInst struct {
	input StatelessInput
}

var _ InputManager = StatelessInputManager{}

func (_ StatelessInputManager) CreateService(prefix string) (BackgroundService, error) {
	return nil, nil
}

func (sim StatelessInputManager) Create(cfg *common.Config) (Input, error) {
	si, err := sim.Configure(cfg)
	if err != nil {
		return nil, err
	}
	return statelessInputInst{si}, nil
}

func (si statelessInputInst) Run(ctx Context, pipeline beat.PipelineConnector) (err error) {
	defer func() {
		if v := recover(); v != nil {
			if e, ok := v.(error); ok {
				err = e
			} else {
				err = fmt.Errorf("input panic with: %+v\n%s", v, debug.Stack())
			}
		}
	}()

	client, err := pipeline.ConnectWith(beat.ClientConfig{
		PublishMode: beat.DefaultGuarantees,

		// configure pipeline to disconnect input on stop signal.
		CloseRef: ctx.Cancelation,
		Processing: beat.ProcessingConfig{
			// Note: still required for autodiscovery. These kind of processing setups
			// should move to autodiscovery in the future, but is required to be applied
			// here for now to keep compatibility with other beats.
			DynamicFields: ctx.Metadata,
		},
	})
	if err != nil {
		return err
	}

	defer client.Close()
	return si.input.Run(ctx, client.Publish)
}

func (si statelessInputInst) Test(ctx TestContext) error {
	return si.input.Test(ctx)
}
