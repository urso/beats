package inputs

import (
	"github.com/elastic/beats/v7/filebeat/beater"
	ossinputs "github.com/elastic/beats/v7/filebeat/features/input/default-inputs"
	v2 "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/x-pack/filebeat/features/input/o365"
)

func Init(info beat.Info, log *logp.Logger, store beater.StateStore) *v2.Registry {
	return v2.NewRegistry(
		ossinputs.Init(info, log, store),
		xpackInputs(info, log, store),
	)
}

func xpackInputs(info beat.Info, log *logp.Logger, store beater.StateStore) *v2.Registry {
	return v2.NewRegistry(
		o365.Plugin(log, store),
	)
}
