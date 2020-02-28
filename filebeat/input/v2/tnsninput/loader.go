package tnsninput

import (
	v2 "github.com/elastic/beats/filebeat/input/v2"
	"github.com/elastic/beats/libbeat/common"
)

// Loader creates inputs from a configuration by finding and calling the right
// plugin.
type Loader struct {
	typeField string
	reg       *Registry
}

var _ v2.Loader = (*Loader)(nil)

// NewLoader creates a new Loader for a registry.
func NewLoader(typeField string, reg *Registry) *Loader {
	if typeField == "" {
		typeField = "type"
	}
	return &Loader{typeField, reg}
}

// Configure looks for a plugin matching the 'type' name in the configuration
// and creates a new Input.  Configure fails if the type is not known, or if
// the plugin can not apply the configuration.
func (l *Loader) Configure(cfg *common.Config) (v2.Input, error) {
	name, err := cfg.String(l.typeField, -1)
	if err != nil {
		return v2.Input{}, err
	}

	plugin, ok := l.reg.findPlugin(name)
	if !ok {
		return v2.Input{}, &v2.LoaderError{Name: name, Reason: v2.ErrUnknown}
	}

	return plugin.Create(cfg)
}
