package fbossinputs

import (
	"github.com/elastic/beats/v7/filebeat/features/input/tcp"
	v2 "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/registry"
)

func Inputs(info beat.Info, log *logp.Logger, reg *registry.Registry) *v2.Registry {
	return v2.NewRegistry(
		// list of inputs for the current OS build target
		osInputs(info, log, reg),

		// list of os unspecific inputs
		tcp.Plugin(),
	)
}
