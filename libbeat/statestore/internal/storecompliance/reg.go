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

package storecompliance

import (
	"testing"

	"github.com/elastic/beats/v7/libbeat/statestore/backend"
)

type Registry struct {
	T testing.TB
	backend.Registry
}

type Store struct {
	backend.Store

	Registry *Registry
	name     string
}

func (r *Registry) Access(name string) (*Store, error) {
	s, err := r.Registry.Access(name)
	if err != nil {
		return nil, err
	}
	return &Store{Store: s, Registry: r, name: name}, nil
}

func (r *Registry) MustAccess(name string) *Store {
	store, err := r.Access(name)
	must(r.T, err, "open store")
	return store
}

// Close closes the testing store.
func (s *Store) Close() {
	err := s.Store.Close()
	must(s.Registry.T, err, "closing store %q failed", s.name)
}

// ReopenIf reopens the store if b is true.
func (s *Store) ReopenIf(b bool) {
	if b {
		s.Reopen()
	}
}

// Reopen reopens the store by closing the backend store and using the registry
// backend to access the same store again.
func (s *Store) Reopen() {
	t := s.Registry.T

	s.Close()
	if t.Failed() {
		t.Fatal("Test already failed")
	}

	store, err := s.Registry.Registry.Access(s.name)
	must(s.Registry.T, err, "reopen failed")

	s.Store = store
}

func (s *Store) MustHas(key string) bool {
	b, err := s.Has(key)
	must(s.Registry.T, err, "unexpected error on store/has call")
	return b
}

func (s *Store) MustGet(key string, into interface{}) {
	err := s.Get(key, into)
	must(s.Registry.T, err, "unexpected error on store/get call")
}

func (s *Store) MustSet(key string, from interface{}) {
	err := s.Set(key, from)
	must(s.Registry.T, err, "unexpected error on store/set call")
}

func (s *Store) MustRemove(key string) {
	err := s.Store.Remove(key)
	must(s.Registry.T, err, "unexpected error remove key")
}
