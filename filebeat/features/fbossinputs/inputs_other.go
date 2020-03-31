// +build !windows,!linux

package fbossinputs

import (
	v2 "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/registry"
)

func osInputs(info beat.Info, log *logp.Logger, reg *registry.Registry) *v2.Catalog {
	return v2.NewRegistry()
}
