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

	"github.com/elastic/beats/libbeat/registry"
	"github.com/elastic/go-concert/unison"
)

// globalStore is the store managed by Beats. The global store can be updated
// be shared by multiple input controllers.  The log on a key in the global
// store is kept until all update for the key have been written to the
// persistent store.
type globalStore struct {
	locks           *globalLockManager
	persistentStore *registry.Store
}

type globalLockManager struct {
	prefix  string
	manager *unison.LockManager

	mu     sync.Mutex
	active map[string]*unison.ManagedLock
}

func newGlobalStore(name string, lockmgr *unison.LockManager, store *registry.Store) *globalStore {
	return &globalStore{
		locks: &globalLockManager{
			prefix:  name,
			manager: lockmgr,
			active:  map[string]*unison.ManagedLock{},
		},
	}
}

func (s *globalStore) Close() {
	s.locks.Close()
	s.persistentStore.Close()
	*s = globalStore{}
}

func (glm *globalLockManager) Close() {
	invariant(len(glm.active) == 0, "did expect that all locks have been released")
	glm.manager = nil
}

func (glm *globalLockManager) Access(key string) *unison.ManagedLock {
	if glm.prefix != "" {
		key = glm.prefix + "/" + key
	}

	glm.mu.Lock()
	defer glm.mu.Unlock()

	if lock, exist := glm.active[key]; exist {
		return lock
	}

	lock := glm.manager.Access(key)
	glm.active[key] = lock
	return lock
}
