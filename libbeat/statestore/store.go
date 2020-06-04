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
	"github.com/elastic/beats/v7/libbeat/statestore/backend"
	"github.com/elastic/go-concert/atomic"
	"github.com/elastic/go-concert/unison"
)

type sharedStore struct {
	reg      *Registry
	refCount atomic.Int

	name    string
	backend backend.Store
}

// Store instance. The backend is shared between multiple instances of this store.
// The backend will be closed only after the last instance have been closed.
// No transaction can be created once the store instance has been closed.
// A Store is not thread-safe. Each go-routine accessing a store should create
// an instance using `Registry.Get`.
type Store struct {
	shared *sharedStore
	// wait group to ensure active operations can finish, but not started anymore after the store has been closed.
	active unison.SafeWaitGroup
}

func newSharedStore(reg *Registry, name string, backend backend.Store) *sharedStore {
	return &sharedStore{
		reg:      reg,
		refCount: atomic.MakeInt(1),
		name:     name,
		backend:  backend,
	}
}

func newStore(shared *sharedStore) *Store {
	shared.Retain()
	return &Store{
		shared: shared,
	}
}

// Close deactivates the current store. No new transacation can be generated.
// Already active transaction will continue to function until Closed.
// The backing store will be closed once all stores and active transactions have been closed.
func (s *Store) Close() error {
	if err := s.active.Add(1); err != nil {
		return &ErrorClosed{operation: "store/close", name: s.shared.name}
	}
	s.active.Close()

	s.active.Wait()
	return s.shared.Release()
}

func (s *Store) Has(key string) (bool, error) {
	const operation = "store/has"
	if err := s.active.Add(1); err != nil {
		return false, &ErrorClosed{operation: operation, name: s.shared.name}
	}
	defer s.active.Done()

	has, err := s.shared.backend.Has((key))
	if err != nil {
		return false, &ErrorOperation{name: s.shared.name, operation: operation, cause: err}
	}
	return has, nil
}

func (s *Store) Get(key string, into interface{}) error {
	const operation = "store/get"
	if err := s.active.Add(1); err != nil {
		return &ErrorClosed{operation: operation, name: s.shared.name}
	}
	defer s.active.Done()

	err := s.shared.backend.Get(key, into)
	if err != nil {
		return &ErrorOperation{name: s.shared.name, operation: operation, cause: err}
	}
	return nil
}

func (s *Store) Set(key string, from interface{}) error {
	const operation = "store/get"
	if err := s.active.Add(1); err != nil {
		return &ErrorClosed{operation: operation, name: s.shared.name}
	}
	defer s.active.Done()

	if err := s.shared.backend.Set((key), from); err != nil {
		return &ErrorOperation{name: s.shared.name, operation: operation, cause: err}
	}
	return nil
}

/*
func (s *Store) Update(key string, value interface{}) error {
	const operation = "store/update"
	if err := s.activeTx.Add(1); err != nil {
		return &ErrorClosed{operation: operation, name: s.shared.name}
	}
	defer s.activeTx.Done()

	if err := s.shared.backend.Update((key), value); err != nil {
		return &ErrorOperation{name: s.shared.name, operation: operation, cause: err}
	}
	return nil
}
*/

func (s *Store) Remove(key string) error {
	const operation = "store/remove"
	if err := s.active.Add(1); err != nil {
		return &ErrorClosed{operation: operation, name: s.shared.name}
	}
	defer s.active.Done()

	if err := s.shared.backend.Remove((key)); err != nil {
		return &ErrorOperation{name: s.shared.name, operation: operation, cause: err}
	}
	return nil
}

func (s *Store) Each(fn func(string, ValueDecoder) (bool, error)) error {
	if err := s.active.Add(1); err != nil {
		return &ErrorClosed{operation: "store/each", name: s.shared.name}
	}
	defer s.active.Done()

	return s.shared.backend.Each(fn)
}

func (s *sharedStore) Retain() {
	s.refCount.Inc()
}

func (s *sharedStore) Release() error {
	if s.refCount.Dec() == 0 && s.tryUnregister() {
		return s.backend.Close()
	}
	return nil
}

// tryUnregister removed the store from the registry. tryUnregister returns false
// if the store has been retained in the meantime. True is returned if the store
// can be closed for sure.
func (s *sharedStore) tryUnregister() bool {
	s.reg.mu.Lock()
	defer s.reg.mu.Unlock()
	if s.refCount.Load() > 0 {
		return false
	}

	s.reg.unregisterStore(s)
	return true
}

func ifNot(b *bool, fn func()) {
	if !(*b) {
		fn()
	}
}
