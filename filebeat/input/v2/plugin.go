package v2

import (
	"fmt"

	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/feature"
)

type Plugin struct {
	Name       string
	Stability  feature.Stability
	Deprecated bool
	Info       string
	Doc        string
	Manager    InputManager
}

var _ Addon = Plugin{}
var _ Extension = Plugin{}

func (p Plugin) Details() feature.Details {
	return feature.Details{
		Name:       p.Name,
		Stability:  p.Stability,
		Deprecated: p.Deprecated,
		Info:       p.Info,
		Doc:        p.Doc,
	}
}

func (p Plugin) Configure(cfg *common.Config) (Input, error) {
	return p.Manager.Create(cfg)
}

func (p Plugin) addToRegistry(parent *Registry) {
	if parent.plugins == nil {
		parent.plugins = make(map[string]Plugin)
	}
	if _, exists := parent.plugins[p.Name]; exists {
		panic(fmt.Errorf("Plugin %v already exists", p.Name))
	}
	parent.plugins[p.Name] = p
}
