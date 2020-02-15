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

// Package statestore provides coordinated access to entries in the registry
// via resources.
//
// A Store supports selective update of fields using go structures.
// Data to be stored in the registry should be split into resource identifying
// meta-data and read state data. This allows inputs to have separate
// go-routines for updating resource tracking meta-data (like file path upon
// file renames) and for read state updates (like file offset).
//
// The registry is only eventually consistent to the current state of the
// store. When using (*Resource).Update, both the in-memory state and the
// registry state will be updated immediately. But when using
// (*Resource).UpdateOp, only the in memory state will be updated. The
// registry state must be updated using the ResourceUpdateOp, after the
// associated events have been ACKed by the outputs. Once all pending update operations have been applied
// the in-memory state and the persistent state are assumed to be in-sync, and
// the in-memory state is dropped so to free some memory.
//
// The eventual consistency allows resources to be Unlocked and Locked by another go-routine
// immediately, as the final read state from the former go-routine is available
// right away. The lock guarantees exclusive access. In the meantime older
// updates might still be applied to the registry file, while the new
// go-routine can start creating new update operations concurrently to be
// applied to after already pending updates.
package statestore

import (
	"github.com/elastic/beats/libbeat/registry"
	"github.com/elastic/go-concert"
	"github.com/elastic/go-concert/atomic"
)

// ResourceKey is used to describe an unique resource to be stored in the registry.
type ResourceKey string

// Store provides some coordinates access to a registry.Store.
// All update and read operations require users to acquire a resource first.
// A Resource must be locked before it can be modified. This ensures that at most
// one go-routine has access to a resource. Lock/TryLock/Unlock can be used to
// coordinate resource access even between independent components.
//
// Updates in the shared store can be ahead of the globalStore. Updates will be eventually synchronised to the global store
// via update operations that are finally executed. Locks already released on the shared store are not yet released in the global store,
type Store struct {
	active atomic.Bool

	session *storeSession
}

// storeSession keeps track of the lifetime of a Store instance.
// In flight resource update operations do extend the lifetime of
// a Store, even if the store has been closed by a go-routine.
//
// A session will shutdown when the Store is closed and all pending
// update operations have been persisted. The session outlives the 'Store' in
// the case that update operations are still pending when the store is closed.
type storeSession struct {
	refCount concert.RefCount

	local  *shadowStore
	global *registry.Store

	// keep track of owner, so we can remove close the shared store once the last
	// session goes away.
	connector *Connector
}

// NewStore creates a new Store.
func newStore(session *storeSession) *Store {
	invariant(session != nil, "missing a persistent store")

	return &Store{
		active:  atomic.MakeBool(true),
		session: session,
	}
}

// Close deactivates the store and waits for all resources to be released.
// Resources can not be accessed anymore, but in progress resource updates are
// still active, until they are eventually ACKed.  The underlying persistent
// store will be finally closed once all pending updates have been written to
// the persistent store.
func (s *Store) Close() {
	s.active.Store(false)
	s.session.Release()
}

// Access an unlocked resource. This creates a handle to a resource that may
// not yet exist in the persistent registry.
func (s *Store) Access(key ResourceKey) *Resource {
	return newResource(s, key)
}

func (s *Store) isOpen() bool {
	return s.active.Load()
}

func (s *Store) isClosed() bool {
	return !s.isOpen()
}

func newSession(
	connector *Connector,
	global *registry.Store,
	local *shadowStore,
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
