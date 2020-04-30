package fbossinputs

import (
	"github.com/elastic/beats/v7/filebeat/features/input/journald"
	v2 "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/filebeat/input/v2/exclinput"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/logp"
)

// inputs that are only supported on linux

type osComponents interface {
	exclinput.StateStore
}

func osInputs(info beat.Info, log *logp.Logger, components osComponents) *v2.Registry {
	return v2.NewRegistry(
		journald.Plugin(log, components),
	)
}
