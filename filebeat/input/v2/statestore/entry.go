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
	mux     sync.Mutex
	pending uint          // pending updates until value is in sync
	cached  common.MapStr // current value if state == valueOutOfSync
}

func (r *resourceEntry) Lock() {
	r.globalLock.Lock()
	r.mu.Lock()
}

func (r *resourceEntry) TryLock() bool {
	if !r.globalLock.TryLock() {
		return false
	}

	if !r.mu.TryLock() {
		r.globalLock.Unlock()
		return false
	}

	return true
}

func (r *resourceEntry) Unlock() {
	// Unlock can panic -> ensure we always run globalLock.Unlock()
	defer r.globalLock.Unlock()
	r.mu.Unlock()
}
