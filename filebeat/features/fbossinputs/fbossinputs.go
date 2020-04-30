package fbossinputs

import (
	"github.com/elastic/beats/v7/filebeat/features/input/tcp"
	v2 "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/logp"
)

type Components interface {
	osComponents
}

func Inputs(info beat.Info, log *logp.Logger, components Components) *v2.Registry {
	return v2.NewRegistry(
		genericInputs(),
		osInputs(info, log, components),
	)
}

func genericInputs() *v2.Registry {
	return v2.NewRegistry(
		tcp.Plugin(),
	)
}
