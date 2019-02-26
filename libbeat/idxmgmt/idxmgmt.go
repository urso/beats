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

package idxmgmt

import (
	"errors"
	"fmt"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/idxmgmt/ilm"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/elastic/beats/libbeat/outputs/elasticsearch"
	"github.com/elastic/beats/libbeat/template"
)

// SupportFactory is used to provide custom index management support to libbeat.
type SupportFactory func(*logp.Logger, beat.Info, *common.Config) (Supporter, error)

// Supporter provides index management and configuration related services
// throughout libbeat.
type Supporter interface {
	Enabled() bool

	// ILM provides access to the configured ILM support.
	ILM() ilm.Supporter

	// TemplateConfig returns the template configuration used by the index supporter.
	TemplateConfig(withILM bool) (template.TemplateConfig, error)

	// BuildSelector create an index selector.
	BuildSelector(cfg *common.Config) (outputs.IndexSelector, error)

	// Manager creates a new manager that can be used to execute the required steps
	// for initializing an index, ILM policies, and write aliases.
	Manager(client *elasticsearch.Client, assets Asseter) Manager
}

// Asseter provides access to beats assets required to load the template.
type Asseter interface {
	Fields(name string) []byte
}

// Manager is used to initialize indices, ILM policies, and aliases within the
// Elastic Stack.
type Manager interface {
	Setup(template, policy bool) error
}

// DefaultSupport initializes the default index management support used by most Beats.
func DefaultSupport(log *logp.Logger, info beat.Info, configRoot *common.Config) (Supporter, error) {
	factory := MakeDefaultSupport(nil)
	return factory(log, info, configRoot)
}

// MakeDefaultSupport creates some default index management support, with a
// custom ILM support implementation.
func MakeDefaultSupport(ilmSupport ilm.SupportFactory) SupportFactory {
	if ilmSupport == nil {
		ilmSupport = ilm.DefaultSupport
	}

	return func(log *logp.Logger, info beat.Info, configRoot *common.Config) (Supporter, error) {
		const logName = "index-management"

		cfg := struct {
			ILM       *common.Config         `config:"setup.ilm"`
			Template  *common.Config         `config:"setup.template"`
			Output    common.ConfigNamespace `config:"output"`
			Migration *common.Config         `config:"migration"`
		}{}
		if configRoot != nil {
			if err := configRoot.Unpack(&cfg); err != nil {
				return nil, err
			}
		}

		if log == nil {
			log = logp.NewLogger(logName)
		} else {
			log = log.Named(logName)
		}

		if err := checkTemplateESSettings(cfg.Template, cfg.Output); err != nil {
			return nil, err
		}

		return newIndexSupport(log, info, ilmSupport, cfg.Template, cfg.ILM, cfg.Migration.Enabled())
	}
}

// checkTemplateESSettings validates template settings and output.elasticsearch
// settings to be consistent.
func checkTemplateESSettings(tmpl *common.Config, out common.ConfigNamespace) error {
	if out.Name() != "elasticsearch" {
		return nil
	}

	enabled := tmpl == nil || tmpl.Enabled()
	if !enabled {
		return nil
	}

	var tmplCfg template.TemplateConfig
	if tmpl != nil {
		if err := tmpl.Unpack(&tmplCfg); err != nil {
			return fmt.Errorf("unpacking template config fails: %v", err)
		}
	}

	esCfg := struct {
		Index string `config:"index"`
	}{}
	if err := out.Config().Unpack(&esCfg); err != nil {
		return err
	}

	tmplSet := tmplCfg.Name != "" && tmplCfg.Pattern != ""
	if esCfg.Index != "" && !tmplSet {
		return errors.New("setup.template.name and setup.template.pattern have to be set if index name is modified")
	}

	return nil
}
