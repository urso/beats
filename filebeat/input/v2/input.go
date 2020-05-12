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
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"

	"github.com/elastic/go-concert/unison"
)

// InputManager creates and maintains actions and background processes for an
// input type.
type InputManager interface {
	// Init signals to InputManager to initialize internal resources.
	Init(grp unison.Group, m Mode) error

	// Creates builds a new Input instance from the given configuation, or returns
	// an error if the configuation is invalid.
	// The generated must not collect any data yet. The Beat will use the Test/Run
	// methods of the input.
	Create(*common.Config) (Input, error)
}

// Mode tells the InputManager in which mode it is initialized.
type Mode uint8

//go:generate stringer -type Mode -trimprefix Mode
const (
	ModeRun Mode = iota
	ModeTest
	ModeOther
)

// Input is a configured input object that can be used to probe or start
// the actual data processing.
type Input interface {
	// TODO: check if/how we can remove this method. Currently it is required for
	// compatibility reasons with existing interfaces in libbeat, autodiscovery
	// and filebeat.
	Name() string

	// Test checks the configuaration and runs addition checks if the Input can be
	// initialized from the configuration (e.g. check if host/port or files are
	// accessible).
	Test(TestContext) error

	// Run executes the data collection loop. Run must return an error only if the
	// error can not be recovered.
	Run(Context, beat.PipelineConnector) error
}

// Context provides the Input Run function with common environmental
// information and services.
type Context struct {
	ID          string
	Agent       beat.Info
	Logger      *logp.Logger
	Metadata    *common.MapStrPointer // XXX: from Autodiscovery.
	Cancelation Canceler
}

// TestContext provides the Input Test function with common environmental
// information and services.
type TestContext struct {
	Agent       beat.Info
	Logger      *logp.Logger
	Cancelation Canceler
}

// Canceler is used to provide shutdown handling to the Context.
type Canceler interface {
	Done() <-chan struct{}
	Err() error
}

type simpleInputManager struct {
	configure func(*common.Config) (Input, error)
}

// Init is required to fullfil the input.InputManager interface.
// For the kafka input no special initialization is required.
func (*simpleInputManager) Init(grp unison.Group, m Mode) error { return nil }

// Creates builds a new Input instance from the given configuation, or returns
// an error if the configuation is invalid.
func (manager *simpleInputManager) Create(cfg *common.Config) (Input, error) {
	return manager.configure(cfg)
}

// ConfigureWith creates an InputManager that creates new inputs with the given function.
func ConfigureWith(fn func(*common.Config) (Input, error)) InputManager {
	return &simpleInputManager{configure: fn}
}
