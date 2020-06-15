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

// Package compat provides helpers for integrating the input/v2 API with
// existing input based features like autodiscovery, config file reloading, or
// filebeat modules.
package compat

import (
	"sync"

	v2 "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/cfgfile"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/go-concert"
)

type factory struct {
	log    *logp.Logger
	info   beat.Info
	loader *v2.Loader
}

type runner struct {
	log       *logp.Logger
	agent     *beat.Info
	wg        sync.WaitGroup
	sig       *concert.OnceSignaler
	input     v2.Input
	connector beat.PipelineConnector
}

// RunnerFactory creates a cfgfile.RunnerFactory from an input Loader that is
// compatible with cfgfile runners, autodiscovery, and filebeat modules.
func RunnerFactory(
	log *logp.Logger,
	info beat.Info,
	loader *v2.Loader,
) cfgfile.RunnerFactory {
	return &factory{log: log, info: info, loader: loader}
}

func (f *factory) CheckConfig(cfg *common.Config) error {
	_, err := f.loader.Configure(cfg)
	if err != nil {
		return err
	}
	return nil
}

func (f *factory) Create(
	p beat.PipelineConnector,
	config *common.Config,
	meta *common.MapStrPointer,
) (cfgfile.Runner, error) {
	input, err := f.loader.Configure(config)
	if err != nil {
		return nil, err
	}

	return &runner{
		log:       f.log.Named(input.Name()),
		agent:     &f.info,
		sig:       concert.NewOnceSignaler(),
		input:     input,
		connector: p,
	}, nil
}

func (r *runner) String() string { return r.input.Name() }

func (r *runner) Start() {
	log := r.log
	name := r.input.Name()

	go func() {
		log.Infof("Input %v starting", name)
		err := r.input.Run(
			v2.Context{
				ID:          "", // TODO: hmmm....
				Agent:       *r.agent,
				Logger:      log,
				Cancelation: r.sig,
			},
			r.connector,
		)
		if err != nil {
			log.Errorf("Input '%v' failed with: %+v", name, err)
		} else {
			log.Infof("Input '%v' stopped", name)
		}
	}()
}

func (r *runner) Stop() {
	r.sig.Trigger()
	r.wg.Wait()
	r.log.Infof("Input '%v' stopped", r.input.Name())
}
