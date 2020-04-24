// +build !windows,!linux

package fbossinputs

import (
	v2 "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/statestore"
)

func osInputs(info beat.Info, log *logp.Logger, reg *statestore.Registry) *v2.Registry {
	return v2.NewRegistry()
}
