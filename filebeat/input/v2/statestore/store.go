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
)

type shadowStore struct {
	refCount concert.RefCount
	name     string
	entries  table
}

// sharedStore is the shared store instance as is tracked by the Connector. The
// shared store is local to the input controller that create the
// statestore.Store instance.
// Any two go-routines accessing a Store will reference the same underlying store.
// If one of the go-routines closes the store, we ensure that the shared resources
// are kept alive until all go-routines have given up access.
type sharedStore struct {
	refCount     concert.RefCount
	name         string
	resourcesMux sync.Mutex
	resources    table
}

func (s *sharedStore) close() error {
	err := s.persistentStore.Close()
	s.persistentStore = nil
	return err
}

func (s *sharedStore) releaseEntry(entry *resourceEntry) {
	s.resourcesMux.Lock()
	defer s.resourcesMux.Unlock()
	if entry.refCount.Release() {
		s.remove(entry.key, lockAlreadyTaken)
	}
}

func (s *sharedStore) findOrCreate(key ResourceKey, lm lockMode) (res *resourceEntry) {
	withLockMode(&s.resourcesMux, lm, func() error {
		if res = s.resources.Find(key); res == nil {
			res = s.resources.Create(key)
		}
		return nil
	})
	return res
}

func (s *sharedStore) find(key ResourceKey, lm lockMode) (res *resourceEntry) {
	withLockMode(&s.resourcesMux, lm, func() error {
		res = s.resources.Find(key)
		return nil
	})
	return res
}

func (s *sharedStore) create(key ResourceKey, lm lockMode) (res *resourceEntry) {
	withLockMode(&s.resourcesMux, lm, func() error {
		res = s.resources.Create(key)
		return nil
	})
	return res
}

func (s *sharedStore) remove(key ResourceKey, lm lockMode) {
	withLockMode(&s.resourcesMux, lm, func() error {
		s.resources.Remove(key)
		return nil
	})
}
