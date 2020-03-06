package urlcollect

import (
	"errors"
	"fmt"
	"net/url"

	v2 "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/feature"
)

// Loader create new inputs.
type Loader struct {
	TypeField string
	Manager   *InputManager
	Registry  *Registry
}

// Extension types can be Registry or Plugin. It is used to combine plugins and
// registry into an even bigger registry.
// for Example:
//    r1, _ := NewRegistry(...)
//    r2, _ := NewRegistry(...)
//    r, err := NewRegistry(r1, r2,
//        &Plugin{...},
//        &Plugin{...},
//    )
//    // -> r can be used to load plugins from all registries and plugins
//    //    added.
type Extension interface {
	addToRegistry(reg *Registry) error
}

// Registry is used to lookup available plugins.
type Registry v2.RegistryTree

// Plugin to be added to a registry. The Plugin will be used to create an
// Input. The inputs run and test functions will be executed per configured URL.
type Plugin struct {
	Name       string
	Stability  feature.Stability
	Deprecated bool
	Info       string
	Doc        string
	Create     func(*common.Config) ([]*url.URL, Input, error)
}

// Input is created by the plugin. Inputs can optionally provide some testig functionality.
type Input struct {
	Run  InputFunc
	Test TestFunc
}

type InputFunc func(v2.Context, *InputManager, *url.URL) error

type TestFunc func(v2.TestContext, *url.URL) error

var _ v2.Loader = (*Loader)(nil)
var _ v2.Plugin = (*Plugin)(nil)
var _ v2.Registry = (*Registry)(nil)
var _ Extension = (*Plugin)(nil)
var _ Extension = (*Registry)(nil)

var errNoEndpointConfigured = errors.New("no address configured")

// Configure creates an Input from the configuration available in the PLugin registry.
// Configure fails if the plugin is not known, or the plugin finds the configuration to be invalid.
func (l *Loader) Configure(cfg *common.Config) (v2.Input, error) {
	required(l.Manager != nil, "no input manager set")
	required(l.Registry != nil, "no registry set")

	typeField := l.getTypeField()
	name, err := getPluginName(typeField, cfg)
	if err != nil {
		return v2.Input{}, &v2.LoaderError{
			Reason:  v2.ErrNoInputConfigured,
			Message: fmt.Sprintf("%v setting is missing", typeField),
		}
	}

	plugin, ok := l.Registry.findPlugin(name)
	if !ok {
		return v2.Input{}, &v2.LoaderError{Name: name, Reason: v2.ErrUnknown}
	}

	urls, input, err := plugin.Create(cfg)
	if err != nil {
		return v2.Input{}, &v2.LoaderError{Name: name, Reason: err}
	}
	if len(urls) == 0 {
		return v2.Input{}, &v2.LoaderError{Name: name, Reason: errNoEndpointConfigured}
	}

	return l.castInput(plugin.Name, urls, input), nil
}

func (l *Loader) castInput(name string, urls []*url.URL, input Input) v2.Input {
	managedInput := &managedInput{
		urls:       urls,
		pluginName: name,
		manager:    l.Manager,
		input:      input,
	}
	return v2.Input{
		Name: name,
		Run:  managedInput.Run,
		Test: managedInput.Test,
	}
}

func (l *Loader) getTypeField() string {
	if l.TypeField == "" {
		return "input"
	}
	return l.TypeField
}

// NewRegistry creates a new Registry from the list of Registrations and Plugins.
func NewRegistry(exts ...Extension) (*Registry, error) {
	r := &Registry{}
	for _, ext := range exts {
		if err := r.Add(ext); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// Add adds another registry or plugin to the current registry.
func (r *Registry) Add(ext Extension) error { return ext.addToRegistry(r) }

func (r *Registry) addToRegistry(parent *Registry) error {
	return (*v2.RegistryTree)(parent).AddRegistry(r)
}

func (p *Plugin) addToRegistry(parent *Registry) error {
	return (*v2.RegistryTree)(parent).AddPlugin(p)
}

// Each iterates over all known plugins accessible using this registry.
// The iteration stops when fn return false.
func (r *Registry) Each(fn func(v2.Plugin) bool) {
	(*v2.RegistryTree)(r).Each(fn)
}

// Find returns a Plugin based on it's name.
func (r *Registry) Find(name string) (plugin v2.Plugin, ok bool) {
	return (*v2.RegistryTree)(r).Find(name)
}

func (r *Registry) findPlugin(name string) (*Plugin, bool) {
	p, ok := r.Find(name)
	if !ok {
		return nil, false
	}
	return p.(*Plugin), ok
}

// Details returns common feature information about the plugin the and input
// type it can generate.
func (p *Plugin) Details() feature.Details {
	return feature.Details{
		Name:       p.Name,
		Stability:  p.Stability,
		Deprecated: p.Deprecated,
		Info:       p.Info,
		Doc:        p.Doc,
	}
}

func getPluginName(typeField string, cfg *common.Config) (string, error) {
	if typeField == "" {
		typeField = "input"
	}
	return cfg.String(typeField, -1)
}

func required(b bool, msg string) {
	if !b {
		panic(errors.New(msg))
	}
}
