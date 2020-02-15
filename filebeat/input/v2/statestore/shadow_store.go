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

	"github.com/elastic/go-concert"
	"github.com/elastic/go-concert/unison"
)

// shadowStore is the shared store instance as is tracked by the Connector. The
// shadow store is local to the input controller that create the
// statestore.Store instance.
// Any two go-routines accessing a Store will reference the same underlying store.
// If one of the go-routines closes the store, we ensure that the shared resources
// are kept alive until all go-routines have given up access.
//
// The shadow store keeps track of pending updates and ensures that locks within the global lock manager
// are held for as long as is required. This is done by keeping an in memory
// registry state table. Updates are written directly to this table.  As long
// as there are pending operations, we read the state from this table.  If
// there is no entry with cached values present, we assume the registry to be in
// sync with all updates applied.  Entries are reference counted allowing us to
// free space in the table if there is no more go-routine potentially accessing
// a resource.
type shadowStore struct {
	refCount concert.RefCount
	name     string

	// locksManager is responsible for keeping track of global locks in the user provided
	// LockManager.
	locksManager *globalLockManager

	// entries shadowing entries in the registry. Entries in this table
	// are ahead of entries in the Registry.
	resourcesMux sync.Mutex
	resources    map[ResourceKey]*resourceEntry
}

// globalLockManager gives access to the LockManager. It ensures that the global lock is
// locked for as long as any session holds the lock.
type globalLockManager struct {
	prefix  string
	manager *unison.LockManager

	mu     sync.Mutex
	active map[string]*globalLock
}

// globalLock keeps track of lock/unlocks within shared sessions, ensuring that the managed lock
// from the global LockManager is kept for as long as at least one entry in the StateStore
// require the lock. The global lock will be freed after all session that do access the lock
// have been finished.
type globalLock struct {
	name      string
	lock      *unison.ManagedLock
	mu        sync.Mutex
	lockCount int
	ref       concert.RefCount
}

func newShadowStore(name string, lockmngr *unison.LockManager) *shadowStore {
	return &shadowStore{
		name:         name,
		locksManager: newGlobalLockManager(name, lockmngr),
		resources:    map[ResourceKey]*resourceEntry{},
	}
}

func (s *shadowStore) close() error {
	s.locksManager.Close()
	return nil
}

func (s *shadowStore) releaseEntry(entry *resourceEntry) {
	s.resourcesMux.Lock()
	defer s.resourcesMux.Unlock()
	if entry.refCount.Release() {
		s.remove(entry.key, lockAlreadyTaken)
	}
}

func (s *shadowStore) findOrCreate(key ResourceKey, lm lockMode) (res *resourceEntry) {
	withLockMode(&s.resourcesMux, lm, func() error {
		if res = s.find(key, lockAlreadyTaken); res == nil {
			res = s.createEntry(key)
			s.resources[key] = res
		}
		return nil
	})
	return res
}

func (s *shadowStore) find(key ResourceKey, lm lockMode) (res *resourceEntry) {
	withLockMode(&s.resourcesMux, lm, func() error {
		res = s.resources[key]
		if res != nil {
			res.refCount.Retain()
		}
		return nil
	})
	return res
}

func (s *shadowStore) create(key ResourceKey, lm lockMode) (res *resourceEntry) {
	withLockMode(&s.resourcesMux, lm, func() error {
		res = s.createEntry(key)
		s.resources[key] = res
		return nil
	})
	return res
}

func (s *shadowStore) remove(key ResourceKey, lm lockMode) {
	withLockMode(&s.resourcesMux, lm, func() error {
		delete(s.resources, key)
		return nil
	})
}

func (s *shadowStore) createEntry(key ResourceKey) *resourceEntry {
	lock := s.locksManager.Access(string(key))
	return &resourceEntry{
		key:        key,
		mu:         unison.MakeMutex(),
		globalLock: lock,
	}
}

func newGlobalLockManager(name string, lockmngr *unison.LockManager) *globalLockManager {
	return &globalLockManager{
		prefix:  name,
		manager: lockmngr,
		active:  map[string]*globalLock{},
	}
}

func (glm *globalLockManager) Close() {
	invariant(len(glm.active) == 0, "did expect that all locks have been released")
	glm.manager = nil
}

func (glm *globalLockManager) Access(key string) *globalLock {
	if glm.prefix != "" {
		key = glm.prefix + "/" + key
	}

	glm.mu.Lock()
	defer glm.mu.Unlock()

	if lock, exist := glm.active[key]; exist {
		lock.ref.Retain()
		return lock
	}

	lock := newGlobalLock(key, glm.manager.Access(key))
	glm.active[key] = lock
	return lock
}

func (glm *globalLockManager) releaseLock(l *globalLock) {
	glm.mu.Lock()
	defer glm.mu.Unlock()

	if l.ref.Release() {
		if l.lockCount > 0 {
			l.lock.Unlock()
		}

		delete(glm.active, l.name)
	}
}

func newGlobalLock(name string, lock *unison.ManagedLock) *globalLock {
	return &globalLock{name: name, lock: lock}
}

func (l *globalLock) Lock() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.lockCount > 0 {
		l.lockCount++
		return
	}

	l.lock.Lock()
	l.lockCount++
}

func (l *globalLock) TryLock() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.lockCount > 0 {
		return true
	}

	success := l.lock.TryLock()
	if success {
		l.lockCount++
	}
	return success
}

func (l *globalLock) Unlock() {
	l.mu.Lock()
	defer l.mu.Unlock()

	invariant(l.lockCount > 0, "attempting to unlock unlocked global store lock")

	l.lockCount--
	if l.lockCount == 0 {
		l.lock.Unlock()
	}
}
