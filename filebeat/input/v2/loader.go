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
	"github.com/elastic/go-concert/unison"
	"github.com/urso/sderr"
)

type Loader struct {
	registry    *Registry
	typeField   string
	defaultType string
}

func NewLoader(registry *Registry, typeField, defaultType string) (*Loader, error) {
	required(registry != nil, "no registry set")
	if typeField == "" {
		typeField = "type"
	}

	if err := ValidateRegistry(registry); err != nil {
		return nil, err
	}

	return &Loader{
		registry:    registry,
		typeField:   typeField,
		defaultType: defaultType,
	}, nil
}

func (l *Loader) Init(group unison.Group, mode Mode) error {
	var err error
	l.registry.Each(func(p Plugin) bool {
		err = p.Manager.Init(group, mode)
		return err == nil
	})
	return err
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

	p, err := l.registry.Find(name)
	if err != nil {
		return nil, &LoaderError{Name: name, Reason: err}
	}

	return p.Configure(cfg)
}

func required(b bool, msg string) {
	if !b {
		panic(errors.New(msg))
	}
}

// ValidateRegistry checks if there are multiple plugins with the same name
// in the registry.
func ValidateRegistry(c *Registry) error {
	seen := common.StringSet{}
	dups := map[string]int{}

	// recursively look for duplicate entries.
	c.Each(func(p Plugin) bool {
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
