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

package cursor

import (
	"time"

	input "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/beat"
)

type Publisher interface {
	Publish(event beat.Event, cursor interface{}) error
}

type cursorPublisher struct {
	ctx    *input.Context
	client beat.Client
	cursor *Cursor
}

type updateOp struct {
	store     *store
	resource  *resource
	timestamp time.Time
	ttl       time.Duration
	delta     interface{}
}

func (c *cursorPublisher) Publish(event beat.Event, cursorUpdate interface{}) error {
	if cursorUpdate == nil {
		return c.forward(event)
	}

	op, err := createUpdateOp(c.cursor.store, c.cursor.resource, cursorUpdate)
	if err != nil {
		return err
	}

	event.Private = op
	return c.forward(event)
}

func (c *cursorPublisher) forward(event beat.Event) error {
	c.client.Publish(event)
	return c.ctx.Cancelation.Err()
}

func createUpdateOp(store *store, resource *resource, updates interface{}) (*updateOp, error) {
	ts := time.Now()

	if err := resource.UpdateCursor(updates); err != nil {
		return nil, err
	}

	resource.Retain()

	resource.state.Internal.Updated = ts
	return &updateOp{
		resource:  resource,
		store:     store,
		timestamp: ts,
		delta:     updates,
	}, nil
}

func (op *updateOp) Execute() {
	defer op.resource.Release()
	op.store.UpdateCursor(op.resource, op.timestamp, op.delta)
	*op = updateOp{}
}
