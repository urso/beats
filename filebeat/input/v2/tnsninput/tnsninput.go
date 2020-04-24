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

package tnsninput

import (
	"fmt"
	"runtime/debug"

	v2 "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
)

// Manager provides an InputManager for transient inputs, that do not store
// state in the registry or require end-to-end event acknowledgement.
type Manager struct {
	Configure func(*common.Config) (Input, error)
}

// Input is the interface transient inputs are required to implemented.
type Input interface {
	Name() string
	Test(v2.TestContext) error
	Run(ctx v2.Context, publish func(beat.Event)) error
}

type statelessInputInst struct {
	input Input
}

var _ v2.InputManager = Manager{}

func (m Manager) Start(_ v2.Mode) error { return nil }
func (m Manager) Stop()                 {}

// Create configures a transient input and ensures that the final input can be used with
// with the filebeat input architecture.
func (m Manager) Create(cfg *common.Config) (v2.Input, error) {
	inp, err := m.Configure(cfg)
	if err != nil {
		return nil, err
	}
	return statelessInputInst{inp}, nil
}

func (si statelessInputInst) Name() string { return si.input.Name() }

func (si statelessInputInst) Run(ctx v2.Context, pipeline beat.PipelineConnector) (err error) {
	defer func() {
		if v := recover(); v != nil {
			if e, ok := v.(error); ok {
				err = e
			} else {
				err = fmt.Errorf("input panic with: %+v\n%s", v, debug.Stack())
			}
		}
	}()

	client, err := pipeline.ConnectWith(beat.ClientConfig{
		PublishMode: beat.DefaultGuarantees,

		// configure pipeline to disconnect input on stop signal.
		CloseRef: ctx.Cancelation,
		Processing: beat.ProcessingConfig{
			// Note: still required for autodiscovery. These kind of processing setups
			// should move to autodiscovery in the future, but is required to be applied
			// here for now to keep compatibility with other beats.
			DynamicFields: ctx.Metadata,
		},
	})
	if err != nil {
		return err
	}

	defer client.Close()
	return si.input.Run(ctx, client.Publish)
}

func (si statelessInputInst) Test(ctx v2.TestContext) error {
	return si.input.Test(ctx)
}
