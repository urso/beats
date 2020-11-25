package esbulk

import (
	input "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/feature"
)

const pluginName = "es_bulk"

func Plugin(info beat.Info) input.Plugin {
	return input.Plugin{
		Name:       pluginName,
		Stability:  feature.Experimental,
		Deprecated: false,
		Info:       "Elasticsearch bulk input",
		Doc:        "Use ES bulk API to receive events from Elastic Agent",
		Manager: input.ConfigureWith(func(cfg *common.Config) (input.Input, error) {
			return configure(info, cfg)
		}),
	}
}

func configure(info beat.Info, cfg *common.Config) (input.Input, error) {
	settings, err := readSettings(cfg)
	if err != nil {
		return nil, err
	}

	return newESBulkInput(info.Version, settings)
}
