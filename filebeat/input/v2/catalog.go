package v2

import (
	"github.com/elastic/beats/v7/libbeat/common"
)

// Catalog is a collection of extensions, that can consist of
// other catalogs and plugins.
type Catalog struct {
	plugins map[string]Plugin
	subs    []*Catalog
}

type Addon interface {
	addToCatalog(*Catalog)
}

var _ Addon = (*Catalog)(nil)
var _ Collection = (*Catalog)(nil)

// NewCatalog creates a new catalog from the given catalogs and plugins.
func NewCatalog(extensions ...Addon) *Catalog {
	r := &Catalog{}
	for _, e := range extensions {
		r.Add(e)
	}
	return r
}

func (c *Catalog) addToCatalog(parent *Catalog) {
	parent.subs = append(c.subs, c)
}

// Add adds an existing catalog or plugin.
func (r *Catalog) Add(e Addon) {
	e.addToCatalog(r)
}

// Names returns a sorted list of known plugin names
func (r *Catalog) Names() []string {
	uniq := common.StringSet{}
	r.each(func(p Plugin) bool {
		uniq.Add(p.Name)
		return true
	})
	return uniq.ToSlice()
}

// Each iterates over all known plugins accessible using this catalog.
// The iteration stops when fn return false.
func (r *Catalog) Each(fn func(Extension) (cont bool)) {
	r.each(func(p Plugin) bool { return fn(p) })
}

func (r *Catalog) each(fn func(Plugin) bool) bool {
	// Note: order of Find and each should be in the same order. Direct plugins
	// first followed by sub-catalogs.

	for _, p := range r.plugins {
		if !fn(p) {
			return false
		}
	}

	for _, sub := range r.subs {
		if !sub.each(fn) {
			return false
		}
	}
	return true
}

func (c *Catalog) Find(name string) (Extension, error) {
	plugin, ok := c.find(name)
	if !ok {
		return nil, &LoaderError{Name: name, Reason: ErrUnknown}
	}
	return plugin, nil
}

// Find returns the first Plugin matching the given name.
func (r *Catalog) find(name string) (Plugin, bool) {
	// Note: order of Find and each should be in the same order. Direct plugins
	// first followed by sub-catalogs.

	if p, ok := r.plugins[name]; ok {
		return p, true
	}

	for _, sub := range r.subs {
		if p, ok := sub.find(name); ok {
			return p, ok
		}
	}
	return Plugin{}, false
}

func (c *Catalog) CreateService(prefix string) (BackgroundService, error) {
	var err error
	var services []BackgroundService
	c.each(func(p Plugin) bool {
		var service BackgroundService
		service, err = p.Manager.CreateService(prefix)
		if err == nil && service != nil {
			services = append(services, service)
		}
		return err == nil
	})
	if err != nil {
		return nil, err
	}

	if len(services) == 0 {
		return nil, nil
	}

	return CombineServices(services...), nil
}
