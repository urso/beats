package compat

import (
	"errors"

	v2 "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/cfgfile"
	"github.com/elastic/beats/v7/libbeat/common"
)

type fakeRunnerFactory struct {
	OnCheck  func(*common.Config) error
	OnCreate func(beat.PipelineConnector, *common.Config) (cfgfile.Runner, error)
}

type fakeRunner struct {
	Name    string
	OnStart func()
	OnStop  func()
}

// Create creates a new Runner based on the given configuration.
func (f *fakeRunnerFactory) Create(p beat.PipelineConnector, config *common.Config) (cfgfile.Runner, error) {
	if f.OnCreate == nil {
		return nil, errors.New("not implemented")
	}
	return f.OnCreate(p, config)
}

// CheckConfig tests if a confiugation can be used to create an input. If it
// is not possible to create an input using the configuration, an error must
// be returned.
func (f *fakeRunnerFactory) CheckConfig(config *common.Config) error {
	if f.OnCheck == nil {
		return errors.New("not implemented")
	}
	return f.OnCheck(config)
}

func (f *fakeRunner) String() string { return f.Name }
func (f *fakeRunner) Start() {
	if f.OnStart != nil {
		f.OnStart()
	}
}

func (f *fakeRunner) Stop() {
	if f.OnStop != nil {
		f.OnStop()
	}
}

func constMapRunnerFactory(runners map[string]cfgfile.Runner) cfgfile.RunnerFactory {
	return &fakeRunnerFactory{
		OnCreate: func(_ beat.PipelineConnector, cfg *common.Config) (cfgfile.Runner, error) {
			config := struct {
				Type string
			}{}
			if err := cfg.Unpack(&config); err != nil {
				return nil, err
			}

			runner, ok := runners[config.Type]
			if !ok {
				return nil, v2.ErrUnknown
			}

			return runner, nil
		},
	}
}

func constRunnerFactory(runner cfgfile.Runner) cfgfile.RunnerFactory {
	return &fakeRunnerFactory{
		OnCreate: func(_ beat.PipelineConnector, _ *common.Config) (cfgfile.Runner, error) {
			return runner, nil
		},
	}
}

func failingRunnerFactory(err error) cfgfile.RunnerFactory {
	return &fakeRunnerFactory{
		OnCheck: func(_ *common.Config) error { return err },

		OnCreate: func(_ beat.PipelineConnector, _ *common.Config) (cfgfile.Runner, error) {
			return nil, err
		},
	}
}
