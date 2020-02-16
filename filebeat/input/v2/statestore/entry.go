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

import (
	"sync"

	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/go-concert"
	"github.com/elastic/go-concert/unison"
)

// resourceEntry keeps track of actual resource locks and pending updates.
//
// Note: When locking the resource entry, we first want to lock the globalLock.
//       The global lock keeps track of how many lock attempts we are running, and
//       ensures that the global locks is not released early. When attempting to lock
//       the resource, we need to check if the lock has already been lost by the
//       active lock session of the global lock.
type resourceEntry struct {
	key        ResourceKey
	refCount   concert.RefCount
	globalLock *globalLock
	mu         unison.Mutex
	value      valueState
}

// valueState keeps track of pending updates to a value.
// As long as there are pending updates, cached holds the last known correct value
// and pending will be > 0.
// If `pending` is 0, then the state store and the persistent registry are in sync.
// In this case `cached` will be nil and the registry is used for reading a value.
type valueState struct {
	mux sync.Mutex

	// pending updates until value is in sync
	pendingIgnore uint // ignore updates, because lock was lost
	pendingGood   uint // lock still held

	cached common.MapStr // current value if state == valueOutOfSync
}

func (r *resourceEntry) Lock() *unison.LockSession {
	for {
		session := r.globalLock.Lock()
		err := r.mu.LockContext(channelCtx(session.Done()))
		if err == nil {
			return session
		}
	}
}

func (r *resourceEntry) TryLock() (*unison.LockSession, bool) {
	session, ok := r.globalLock.TryLock()
	if !ok {
		return nil, false
	}

	if !r.mu.TryLock() {
		r.globalLock.Unlock()
		return nil, false
	}

	// we lost the managed lock in the meantime :(
	if !sessionHoldsLock(session) {
		r.mu.Unlock()
		return nil, false
	}
	return session, true
}

func (r *resourceEntry) Unlock() {
	// Unlock can panic -> ensure we always run globalLock.Unlock()
	defer r.globalLock.Unlock()
	r.mu.Unlock()
}
