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
	"fmt"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/common/atomic"
	"github.com/elastic/beats/libbeat/idxmgmt/ilm"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/elastic/beats/libbeat/outputs/elasticsearch"
	"github.com/elastic/beats/libbeat/outputs/outil"
	"github.com/elastic/beats/libbeat/template"
)

type indexSupport struct {
	log          *logp.Logger
	ilm          ilm.Supporter
	info         beat.Info
	migration    bool
	templateCfg  template.TemplateConfig
	defaultIndex string

	st indexState
}

type indexState struct {
	withILM atomic.Bool
}

type indexManager struct {
	support *indexSupport
	ilm     ilm.Manager

	client *elasticsearch.Client
	assets Asseter
}

type indexSelector outil.Selector

type ilmIndexSelector struct {
	index outil.Selector
	alias outil.Selector
	st    *indexState
}

func newIndexSupport(
	log *logp.Logger,
	info beat.Info,
	ilmFactory ilm.SupportFactory,
	tmplConfig *common.Config,
	ilmConfig *common.Config,
	migration bool,
) (*indexSupport, error) {
	if ilmFactory == nil {
		ilmFactory = ilm.DefaultSupport
	}

	ilm, err := ilmFactory(log, info, ilmConfig)
	if err != nil {
		return nil, err
	}

	tmplCfg, err := unpackTemplateConfig(tmplConfig)
	if err != nil {
		return nil, err
	}

	return &indexSupport{
		log:          log,
		ilm:          ilm,
		info:         info,
		templateCfg:  tmplCfg,
		migration:    migration,
		defaultIndex: fmt.Sprintf("%v-%v-%%{+yyyy.MM.dd}", info.IndexPrefix, info.Version),
	}, nil
}

func (s *indexSupport) Enabled() bool {
	return s.templateCfg.Enabled || (s.ilm.Mode() != ilm.ModeDisabled)
}

func (s *indexSupport) ILM() ilm.Supporter {
	return s.ilm
}

func (s *indexSupport) TemplateConfig(withILM bool) (template.TemplateConfig, error) {
	panic("TODO")
}

func (s *indexSupport) Manager(
	client *elasticsearch.Client,
	assets Asseter,
) Manager {
	ilm := s.ilm.Manager(client)
	return &indexManager{
		support: s,
		ilm:     ilm,
		client:  client,
		assets:  assets,
	}
}

func (s *indexSupport) BuildSelector(cfg *common.Config) (outputs.IndexSelector, error) {
	panic("TODO: return ilmIndexSelector")
}

func (m *indexManager) Setup(template, policy bool) error {
	return m.load(template, policy)
}

func (m *indexManager) Load() error {
	return m.load(false, false)
}

func (m *indexManager) load(forceTemplate, forcePolicy bool) error {
	panic("TODO")
}

func (s *ilmIndexSelector) Select(evt *beat.Event) (string, error) {
	if idx := getEventCustomIndex(evt); idx != "" {
		return idx, nil
	}

	if s.st.withILM.Load() {
		idx, err := s.alias.Select(evt)
		return idx, err
	}

	idx, err := s.index.Select(evt)
	return idx, err
}

func (s indexSelector) Select(evt *beat.Event) (string, error) {
	if idx := getEventCustomIndex(evt); idx != "" {
		return idx, nil
	}
	return outil.Selector(s).Select(evt)
}

func getEventCustomIndex(evt *beat.Event) string {
	panic("TODO")
}

func unpackTemplateConfig(cfg *common.Config) (config template.TemplateConfig, err error) {
	config = template.DefaultConfig
	if cfg != nil {
		err = cfg.Unpack(&config)
	}
	return config, err
}
