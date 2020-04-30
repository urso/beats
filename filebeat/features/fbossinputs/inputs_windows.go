package fbossinputs

import (
	v2 "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/logp"
)

type osCompoments interface{}

func osInputs(info beat.Info, log *logp.Logger, components osComponents) *v2.Registry {
	return v2.NewRegistry(
	// TODO: add windows event logs
	)
}
