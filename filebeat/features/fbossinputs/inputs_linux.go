package fbossinputs

import (
	"github.com/elastic/beats/v7/filebeat/features/input/journald"
	v2 "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/statestore"
)

// inputs that are only supported on linux

func osInputs(info beat.Info, log *logp.Logger, reg *statestore.Registry) *v2.Registry {
	return v2.NewRegistry(
		journald.Plugin(log, reg, info.Name),
	)
}
