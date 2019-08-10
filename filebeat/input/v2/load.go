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
	"fmt"

	"github.com/elastic/beats/libbeat/common"
)

type Loader struct {
	inputs map[string]Input
}

func NewLoader(inputs ...Input) (*Loader, error) {
	m := make(map[string]Input, len(inputs))

	for _, input := range inputs {
		if _, exists := m[input.Name]; exists {
			return nil, &Error{
				op:      "loader/new",
				kind:    ErrInitLoader,
				message: fmt.Sprintf("duplicate input %v", input.Name),
			}
		}

		m[input.Name] = input
	}

	return &Loader{inputs: m}, nil
}

func (l *Loader) Find(name string) (Input, bool) {
	input, ok := l.inputs[name]
	return input, ok
}

func (l *Loader) Create(name string, context Context, config *common.Config) (Instance, error) {
	const op = "loader/create"

	input, ok := l.Find(name)
	if !ok {
		return nil, &Error{
			op:      op,
			kind:    ErrUnknownInput,
			name:    name,
			message: fmt.Sprintf("%v not in the table of known inputs", name),
		}
	}

	instance, err := input.Create(context, config)
	if err != nil {
		return nil, &Error{
			op:    op,
			kind:  ErrCreateInput,
			name:  name,
			cause: err,
		}
	}

	return instance, nil
}
