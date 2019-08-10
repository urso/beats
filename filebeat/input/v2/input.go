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
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/monitoring"
)

type Input struct {
	Name   string
	Create func(ctx Context, config *common.Config) (Instance, error)
}

type Context struct {
	Monitors    Monitors
	Pipeline    beat.Pipeline
	Coordinator *Coordinator
}

type Monitors struct {
	Log       *logp.Logger
	Metrics   *monitoring.Registry
	Telemetry *monitoring.Registry
}

type Instance interface {
	Run(ctx Context)
}

type optInstanceStopper interface {
	Stop()
}
