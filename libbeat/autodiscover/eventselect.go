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

package autodiscover

import (
	"errors"

	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/common/bus"
)

type selectedEvents []string

type queryConfigFrom string

// SelectEvents creates an event selector that will filter
// for the configured fields only.
func SelectEvents(events ...string) EventSelector {
	if len(events) == 0 {
		events = []string{"config"}
	}
	return selectedEvents(events)
}

// ConfigEvents creates an EventSelector that will match "config" events only.
func ConfigEvents() EventSelector {
	return SelectEvents("config")
}

func (s selectedEvents) EventFilter() []string {
	return []string(s)
}

// EventConfigQuery creates an EventConfigurer that tries to cast the given event
// field from from the buf event into a []*common.Config.
func EventConfigQuery(field string) EventConfigurer {
	if field == "" {
		field = "config"
	}
	return queryConfigFrom(field)
}

// QueryConfig extract an array of *common.Config from bus.Event.
// The configurations are expected to be in the 'config' field.
func QueryConfig() EventConfigurer { return EventConfigQuery("config") }

func (q queryConfigFrom) CreateConfig(e bus.Event) ([]*common.Config, error) {
	config, ok := e[string(q)].([]*common.Config)
	if !ok {
		return nil, errors.New("Got a wrong value in event `config` key")
	}
	return config, nil
}
