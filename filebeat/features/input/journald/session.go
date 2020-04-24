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

package journald

import (
	"sync"
	"time"
)

type session struct {
	store *store

	mu       sync.Mutex
	cond     *sync.Cond
	closed   bool
	activity uint
}

type updateOp struct {
	session   *session
	resource  *resource
	timestamp time.Time
	ttl       time.Duration
	delta     interface{}
}

func newSession(store *store) *session {
	s := &session{store: store}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// Close waits for updates that are currently executed, and stops all future update events.
func (s *session) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true
	for s.activity > 0 {
		s.cond.Wait()
	}
}

func (s *session) CreateUpdateOp(resource *resource, updates interface{}) (*updateOp, error) {
	ts := time.Now()

	if err := resource.UpdateCursor(updates); err != nil {
		return nil, err
	}

	resource.Retain()

	resource.state.Internal.Updated = ts
	return &updateOp{
		resource:  resource,
		session:   s,
		timestamp: ts,
		delta:     updates,
	}, nil
}

func (s *session) updateBegin() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.activity++
	return true
}

func (s *session) updateEnd() {
	s.mu.Lock()
	s.activity--
	if s.closed && s.activity == 0 {
		s.cond.Signal()
	}
	s.mu.Unlock()
}

func (op *updateOp) Execute() {
	session, resource := op.session, op.resource
	defer resource.Release()

	if session.updateBegin() {
		defer session.updateEnd()
		session.store.UpdateCursor(op.resource, op.timestamp, op.delta)
	}

	*op = updateOp{}
}
