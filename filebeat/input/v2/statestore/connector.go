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

	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/registry"
	"github.com/elastic/go-concert/unison"
)

// Connector is used to connect to a store backed by a registry.
type Connector struct {
	log      *logp.Logger
	lockmngr *unison.LockManager
	registry *registry.Registry

	// set of stores currently with at least one active session.
	mux    sync.Mutex
	stores map[string]*shadowStore
}

// NewConnector creates a new store connector for accessing a resource Store.
func NewConnector(
	log *logp.Logger,
	lockmngr *unison.LockManager,
	reg *registry.Registry,
) *Connector {
	invariant(log != nil, "missing logger")
	invariant(reg != nil, "missing registry")

	return &Connector{
		log:      log,
		lockmngr: lockmngr,
		registry: reg,
		stores:   map[string]*shadowStore{},
	}
}

// Open creates a connection to a named store.
func (c *Connector) Open(name string) (*Store, error) {
	ok := false

	c.mux.Lock()
	defer c.mux.Unlock()

	persistentStore, err := c.registry.Get(name)
	if err != nil {
		return nil, err
	}
	defer ifNotOK(&ok, func() {
		persistentStore.Close()
	})

	shared := c.stores[name]
	if shared == nil {
		shared = newShadowStore(name, c.lockmngr)
		c.stores[name] = shared
	} else {
		shared.refCount.Retain()
	}

	session := newSession(c, persistentStore, shared)

	ok = true
	return newStore(session), nil
}

func (c *Connector) releaseStore(
	global *registry.Store,
	local *shadowStore,
) {
	c.mux.Lock()
	released := local.refCount.Release()
	if released {
		delete(c.stores, local.name)
	}
	c.mux.Unlock()

	if released {
		local.close()
	}
	global.Close()
}
