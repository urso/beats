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
	"sync"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/cfgfile"
	"github.com/elastic/beats/libbeat/common"
)

type RunnerFactory struct {
	loader *Loader
}

type runner struct {
	inst    Instance
	context Context
	wg      sync.WaitGroup
}

func NewRunnerFactory(loader *Loader) *RunnerFactory {
	return &RunnerFactory{
		loader: loader,
	}
}

func (f *RunnerFactory) Create(
	pipeline beat.Pipeline,
	c *common.Config,
	meta *common.MapStrPointer,
) (cfgfile.Runner, error) {
	panic("TODO")
}

func (r *runner) String() string {
	panic("TODO")
}

func (r *runner) Start() {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.inst.Run(r.context)
	}()
}

func (r *runner) Stop() {
	if stopper, ok := r.inst.(optInstanceStopper); ok {
		stopper.Stop()
	}

	// TODO: close context

	r.wg.Wait()
}
