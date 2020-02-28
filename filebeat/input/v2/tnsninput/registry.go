package tnsninput

import (
	"errors"
	"fmt"

	v2 "github.com/elastic/beats/filebeat/input/v2"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/feature"
)

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
type Registry struct {
	plugins    map[string]*Plugin
	registries []*Registry
}

// Plugin to be added to a registry. The Plugin will be used to create an
// Input.
type Plugin struct {
	Name       string
	Stability  feature.Stability
	Deprecated bool
	Info       string
	Doc        string
	Create     func(*common.Config) (v2.Input, error)
}

var _ v2.Plugin = (*Plugin)(nil)
var _ Extension = (*Plugin)(nil)
var _ v2.Registry = (*Registry)(nil)
var _ Extension = (*Registry)(nil)

func (r *Registry) init() {
	if r.plugins == nil {
		r.plugins = map[string]*Plugin{}
	}
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

func (r *Registry) addToRegistry(parent *Registry) error {
	// check my plugins don't exist already
	var err error
	r.Each(func(p v2.Plugin) bool {
		name := p.Details().Name
		_, exists := parent.Find(name)
		if exists {
			err = fmt.Errorf("conflicts with existing '%v' plugin", name)
		}
		return !exists
	})
	if err != nil {
		return err
	}

	parent.registries = append(parent.registries)
	return nil
}

// Add adds another registry or plugin to the current registry.
func (r *Registry) Add(ext Extension) error {
	r.init()
	return ext.addToRegistry(r)
}

// Each iterates over all known plugins accessible using this registry.
// The iteration stops when fn return false.
func (r *Registry) Each(fn func(v2.Plugin) bool) {
	var done bool
	for _, reg := range r.registries {
		if done {
			break
		}

		reg.Each(func(p v2.Plugin) bool {
			done = fn(p)
			return done
		})
	}

	for _, p := range r.plugins {
		if done {
			break
		}
		done = fn(p)
	}
}

// Find returns a Plugin based on it's name.
func (r *Registry) Find(name string) (plugin v2.Plugin, ok bool) {
	return r.findPlugin(name)
}

func (r *Registry) findPlugin(name string) (*Plugin, bool) {
	if p, ok := r.plugins[name]; ok {
		return p, true
	}

	for _, reg := range r.registries {
		if p, ok := reg.findPlugin(name); ok {
			return p, ok
		}
	}
	return nil, false
}

func (p *Plugin) addToRegistry(parent *Registry) error {
	if p.Name == "" {
		panic(errors.New("plugin is missing a name"))
	}
	if p.Create == nil {
		panic(errors.New("plugin has no constructor"))
	}

	if _, exists := parent.Find(p.Name); exists {
		return fmt.Errorf("conflicts with existing '%v' plugin", p.Name)
	}
	parent.plugins[p.Name] = p
	return nil
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
