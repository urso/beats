// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package v2

import (
	"fmt"
	"sort"

	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/feature"
	"github.com/urso/sderr"
)

// Registry that store a number of plugins
type Registry interface {
	Each(func(Plugin) bool)
	Find(name string) (Plugin, bool)
}

// RegistryList combines a list of registries into one registry.
type RegistryList []Registry

// Plugin to be stored in a registry. The plugin reports
// common per plugin details and is used by a loader to create an input.
type Plugin interface {
	Details() feature.Details
}

var _ Registry = (RegistryList)(nil)

// Add adds another registry to the list.
func (l *RegistryList) Add(reg Registry) {
	*l = append(*l, reg)
}

// Validate checks if the registry is valid and does not contain duplicate entries after merging.
func (l RegistryList) Validate() error {
	seen := common.StringSet{}
	dups := map[string]int{}
	l.Each(func(p Plugin) bool {
		name := p.Details().Name
		if seen.Has(name) {
			dups[name]++
		}
		seen.Add(name)
		return true
	})

	if len(dups) == 0 {
		return nil
	}

	var errs []error
	for name, count := range dups {
		errs = append(errs, fmt.Errorf("plugin '%v' found %v time(s)", name, count))
	}
	if len(errs) == 1 {
		return errs[0]
	}

	return sderr.WrapAll(errs, "registry has multiple duplicate plugins")
}

// Names returns asorted list of known plugin names
func (l RegistryList) Names() []string {
	var names []string
	l.Each(func(p Plugin) bool {
		names = append(names, p.Details().Name)
		return true
	})
	sort.Strings(names)
	return names
}

// Find returns the first Plugin matching the given name.
func (l RegistryList) Find(name string) (plugin Plugin, ok bool) {
	for _, reg := range l {
		if p, ok := reg.Find(name); ok {
			return p, ok
		}
	}
	return nil, false
}

// Each iterates over all known plugins
func (l RegistryList) Each(fn func(Plugin) bool) {
	for _, reg := range l {
		reg.Each(fn)
	}
}
