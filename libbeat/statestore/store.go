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
	"github.com/elastic/beats/v7/libbeat/common/atomic"
	"github.com/elastic/beats/v7/libbeat/common/cleanup"
	"github.com/elastic/beats/v7/libbeat/statestore/backend"
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
	shared   *sharedStore
	activeTx unison.SafeWaitGroup // active transaction
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
	if err := s.activeTx.Add(1); err != nil {
		return &ErrorClosed{operation: "store/close", name: s.shared.name}
	}
	s.activeTx.Done()

	s.activeTx.Wait()
	return s.shared.Release()
}

// Begin starts a new transaction within the current store.
// The store ref-counter is increased, such that the final close on a store
// will only happen if all transaction have been closed as well.
// A transaction started with `Begin` must be closed, rolled back or comitted.
// For 'local' transaction and/or guarantees that `Close`, `Rollback`, or `Commit`
// is called correctly use the `Update` or `View` methods.
func (s *Store) Begin(readonly bool) (*Tx, error) {
	const operation = "store/begin-tx"

	if err := s.activeTx.Add(1); err != nil {
		return nil, &ErrorClosed{operation: operation, name: s.shared.name}
	}

	ok := false
	defer cleanup.IfNot(&ok, s.activeTx.Done)

	backendTx, err := s.shared.backend.Begin(readonly)
	if err != nil {
		return nil, &ErrorOperation{name: s.shared.name, operation: operation, cause: err}
	}

	tx := newTx(s.shared.name, backendTx, readonly)
	tx.finishCB = s.finishTx
	ok = true
	return tx, nil
}

// Update runs fn within a writeable transaction. The transaction will be
// rolled back if fn panics or returns an error.
// The transaction will be comitted if fn returns without error.
func (s *Store) Update(fn func(tx *Tx) error) error {
	const operation = "store/update"

	tx, err := s.Begin(false)
	if err != nil {
		return err
	}

	defer tx.Close()
	if err := fn(tx); err != nil {
		tx.Rollback()
		return &ErrorOperation{name: s.shared.name, operation: operation, cause: err}
	}
	return tx.Commit()
}

// View executes a readonly transaction. An error is return if the readonly
// transaction can not be generated or fn returns an error.
func (s *Store) View(fn func(tx *Tx) error) error {
	const operation = "store/view"

	tx, err := s.Begin(true)
	if err != nil {
		return err
	}

	defer tx.Close()
	if err := fn(tx); err != nil {
		return &ErrorOperation{name: s.shared.name, operation: operation, cause: err}
	}
	return nil
}

func (s *Store) finishTx() {
	s.activeTx.Done()
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
