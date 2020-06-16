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

package inputest

import (
	"errors"
	"testing"

	v2 "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/feature"
	"github.com/elastic/go-concert/unison"
)

type MockInputManager struct {
	OnInit      func(v2.Mode) error
	OnConfigure InputConfigurer
}

type InputConfigurer func(*common.Config) (v2.Input, error)

type MockInput struct {
	Type   string
	OnTest func(v2.TestContext) error
	OnRun  func(v2.Context, beat.PipelineConnector) error
}

func makeConfigFakeInput(prototype MockInput) func(*common.Config) (v2.Input, error) {
	return func(cfg *common.Config) (v2.Input, error) {
		tmp := prototype
		return &tmp, nil
	}
}

func (m *MockInputManager) Init(_ unison.Group, mode v2.Mode) error {
	if m.OnInit != nil {
		return m.OnInit(mode)
	}
	return nil
}

func (m *MockInputManager) Create(cfg *common.Config) (v2.Input, error) {
	if m.OnConfigure != nil {
		return m.OnConfigure(cfg)
	}
	return nil, errors.New("oops, OnConfigure not implemented ")
}

func (f *MockInput) Name() string { return f.Type }
func (f *MockInput) Test(ctx v2.TestContext) error {
	if f.OnTest != nil {
		return f.OnTest(ctx)
	}
	return nil
}
func (f *MockInput) Run(ctx v2.Context, pipeline beat.PipelineConnector) error {
	if f.OnRun != nil {
		return f.OnRun(ctx, pipeline)
	}
	return nil
}

func ConstInputManager(input v2.Input) *MockInputManager {
	return &MockInputManager{OnConfigure: ConfigureConstInput(input)}
}

func ConfigureConstInput(input v2.Input) InputConfigurer {
	return func(_ *common.Config) (v2.Input, error) {
		return input, nil
	}
}

func SinglePlugin(name string, manager v2.InputManager) []v2.Plugin {
	return []v2.Plugin{{
		Name:      name,
		Stability: feature.Stable,
		Manager:   manager,
	}}
}

func expectError(t *testing.T, err error) {
	if err == nil {
		t.Errorf("expected error")
	}
}

func expectNoError(t *testing.T, err error) {
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}
