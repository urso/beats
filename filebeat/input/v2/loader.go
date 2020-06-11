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
	"errors"
	"fmt"

	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/feature"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/go-concert/unison"
	"github.com/urso/sderr"
)

type Loader struct {
	log         *logp.Logger
	registry    map[string]Plugin
	typeField   string
	defaultType string
}

func NewLoader(log *logp.Logger, plugins []Plugin, typeField, defaultType string) (*Loader, error) {
	if typeField == "" {
		typeField = "type"
	}

	if err := validatePlugins(plugins); err != nil {
		return nil, err
	}

	registry := make(map[string]Plugin, len(plugins))
	for _, p := range plugins {
		registry[p.Name] = p
	}

	return &Loader{
		log:         log,
		registry:    registry,
		typeField:   typeField,
		defaultType: defaultType,
	}, nil
}

func (l *Loader) Init(group unison.Group, mode Mode) error {
	for _, p := range l.registry {
		if err := p.Manager.Init(group, mode); err != nil {
			return err
		}
	}
	return nil
}

func (l *Loader) Configure(cfg *common.Config) (Input, error) {
	name, err := cfg.String(l.typeField, -1)
	if err != nil {
		if l.defaultType == "" {
			return nil, &LoaderError{
				Reason:  ErrNoInputConfigured,
				Message: fmt.Sprintf("%v setting is missing", l.typeField),
			}
		}
		name = l.defaultType
	}

	p, exists := l.registry[name]
	if !exists {
		return nil, &LoaderError{Name: name, Reason: ErrUnknown}
	}

	log := l.log.With("input", name, "stability", p.Stability, "deprecated", p.Deprecated)
	switch p.Stability {
	case feature.Experimental:
		log.Warnf("EXPERIMENTAL: The %v input is experimental", name)
	case feature.Beta:
		log.Warnf("BETA: The %v input is beta", name)
	}
	if p.Deprecated {
		log.Warnf("DEPRECATED: The %v input is deprecated", name)
	}

	return p.Manager.Create(cfg)
}

func required(b bool, msg string) {
	if !b {
		panic(errors.New(msg))
	}
}

// validatePlugins checks if there are multiple plugins with the same name
// in the registry.
func validatePlugins(plugins []Plugin) error {
	seen := common.StringSet{}
	dups := map[string]int{}

	var errs []error
	for _, p := range plugins {
		name := p.Details().Name

		// check if 'Stability' is configured correctly
		if p.Stability != feature.Undefined {
			errs = append(errs, fmt.Errorf("plugin '%v' has stability not set", name))
		}

		// look for duplicate names.
		if seen.Has(name) {
			dups[name]++
		}
		seen.Add(name)
	}

	if len(dups) == 0 {
		return nil
	}

	for name, count := range dups {
		errs = append(errs, fmt.Errorf("plugin '%v' found %v time(s)", name, count))
	}
	if len(errs) == 1 {
		return errs[0]
	}

	return sderr.WrapAll(errs, "registry has multiple duplicate plugins")
}
