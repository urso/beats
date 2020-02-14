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

package statestore

import "github.com/elastic/go-concert"

// storeSession keeps track of the lifetime of a Store instance.
// In flight resource update operations do extend the lifetime of
// a Store, even if the store has been closed by a go-routine.
//
// A session will shutdown when the Store is closed and all pending
// update operations have been persisted.
type storeSession struct {
	refCount concert.RefCount
	local    *sharedStore
	global   *globalStore

	// keep track of owner, so we can remove close the shared store once the last
	// session goes away.
	connector *Connector
}

func newSession(
	connector *Connector,
	global *globalStore,
	local *sharedStore,
) *storeSession {
	session := &storeSession{connector: connector, global: global, local: local}
	session.refCount.Action = func(_ error) { session.Close() }
	return session
}

func (s *storeSession) Close() {
	s.connector.releaseStore(s.global, s.local)
	s.local = nil
	s.global = nil
	s.connector = nil
}

func (s *storeSession) Retain()       { s.refCount.Retain() }
func (s *storeSession) Release() bool { return s.refCount.Release() }

func (s *storeSession) findOrCreate(key ResourceKey, lm lockMode) (res *resourceEntry) {
	res = s.local.findOrCreate(key, lm)
	return res
}
