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
	"time"

	input "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/go-concert"
	"github.com/elastic/go-concert/chorus"
)

type session struct {
	name     string
	log      *logp.Logger
	owner    sessionOwner
	refCount concert.RefCount
	closer   *chorus.Closer
	store    *store
}

type sessionOwner interface {
	releaseSession(s *session, name string, counter *concert.RefCount) bool
}

type updateOp struct {
	session   *session
	resource  *resource
	timestamp time.Time
	ttl       time.Duration
	delta     interface{}
}

func (s *session) Retain() {
	s.refCount.Retain()
}

func (s *session) Release() {
	var done bool
	if s.owner != nil {
		done = s.owner.releaseSession(s, s.name, &s.refCount)
	} else {
		done = s.refCount.Release()
	}

	if done {
		s.closer.Close()
		s.store.Release()
		s.closer = nil
		s.store = nil
	}
}

// Lock locks a key for exclusive access and returns an resource that can be used to modify
// the cursor state and unlock the key.
func (s *session) Lock(ctx input.Context, key string) (*resource, error) {
	log := ctx.Logger

	resource := s.store.Find(key, true)
	if !resource.lock.TryLock() {
		log.Infof("Resource '%v' currently in use, waiting...", key)
		err := resource.lock.LockContext(ctx.Cancelation)
		if err != nil {
			log.Infof("Input for resource '%v' has been stopped while waiting", key)
			return nil, err
		}
	}
	return resource, nil
}

func (s *session) CreateUpdateOp(resource *resource, updates interface{}) (*updateOp, error) {
	ts := time.Now()

	if err := resource.UpdateCursor(updates); err != nil {
		return nil, err
	}

	resource.Retain()
	s.Retain()

	resource.state.Internal.Updated = ts
	return &updateOp{
		resource:  resource,
		session:   s,
		timestamp: ts,
		delta:     updates,
	}, nil
}

func (s *session) ApplyUpdateOp(op *updateOp) {
	s.store.UpdateCursor(op.resource, op.timestamp, op.delta)
}

func (op *updateOp) Execute() {
	session, resource := op.session, op.resource
	defer session.Release()
	defer resource.Release()

	session.ApplyUpdateOp(op)
	*op = updateOp{}
}
