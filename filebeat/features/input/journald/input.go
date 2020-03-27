package journald

import (
	"github.com/elastic/beats/v7/filebeat/input"
	"github.com/elastic/beats/v7/libbeat/registry"
)

func Plugin(reg *registry.Registry) input.Plugin {
	Name: "journald",
	Stability: feature.Beta,
	Deprecated: false,
	Info: "journald input",
	Doc: "The journald input collects logs from the local journald service",
	Manager: CursorInputManager{
		Registry: reg,
		Configure: func(cfg *common.Config) (CursorInput, error) {
			config := defaultConfig()
			if err := cfg.Unpack(&config); err != nil {
				return nil, err
			}
			return newInput(config)
		},
	},
}

type CursorInputManager struct {
	Registry *registry.Registry
	Configure func(cfg *common.Config) (CursorInput, error)
}

type CursorInput interface {
	Test(input.TestContext) error
}

func newInput(config config) (*journald, error) {
	return nil, errors.New("TODO")
}

func (inp *journald) Test(ctx input.TestContext) error {
}