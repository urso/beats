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
	"runtime"
	"sync"

	"github.com/pkg/errors"

	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/common/transform/typeconv"
	"github.com/elastic/beats/libbeat/registry"
)

type Store struct {
	persistentStore *registry.Store

	resourcesMux sync.Mutex
	resources    table
}

type ResourceKey string

type Resource struct {
	store    *Store
	isLocked bool

	key      ResourceKey
	resource *resource
}

type RegistryResource Resource

type ResourceUpdateOp struct {
	store    *Store
	resource *resource
	key      registry.Key
	val      interface{}
}

func (s *Store) Access(key ResourceKey) *Resource {
	res := &Resource{
		store:    s,
		isLocked: false,
		key:      key,
	}

	// in case we miss an unlock operation (programmer error or panic that hash
	// been caught) we set a finalizer to eventually free the resource.
	// The Unlock operation will unsert the finalizer.
	runtime.SetFinalizer(res, (*Resource).finalize)
	return res
}

// Lock locks and returns the resource for a given key.
func (s *Store) Lock(key ResourceKey) *Resource {
	res := s.Access(key)
	res.Lock()
	return res
}

// TryLock locks and returns the resource for a given key.
// The nil value is returned if the resource could not be locked.
func (s *Store) TryLock(key ResourceKey) *Resource {
	res := s.Access(key)
	if !res.TryLock() {
		res.close()
		return nil
	}
	return res
}

func (r *Resource) Lock() {
	if r.isLocked {
		panic(errors.New("trying to lock already locked resource"))
	}

	store := r.store
	store.resourcesMux.Lock()
	entry := store.resources.GetOrCreate(r.key)
	store.resourcesMux.Unlock()

	entry.Lock()
	r.isLocked = true
	r.resource = entry
}

func (r *Resource) TryLock() bool {
	if r.isLocked {
		panic(errors.New("trying to lock already locked resource"))
	}

	store := r.store
	store.resourcesMux.Lock()
	defer store.resourcesMux.Unlock()

	entry := store.resources.GetOrCreate(r.key)
	r.isLocked = entry.TryLock()

	if !r.isLocked {
		if entry.Release() {
			store.resources.Remove(r.key)
		}
	} else {
		r.resource = entry
	}
	return r.isLocked
}

func (r *Resource) Unlock() {
	if !r.isLocked {
		panic(errors.New("trying to lock unlocked resource"))
	}

	r.close()
}

func (r *Resource) close() {
	runtime.SetFinalizer(r, nil)
	r.finalize()
}

func (r *Resource) finalize() {
	if !r.isLocked {
		return
	}

	store := r.store
	store.resourcesMux.Lock()
	defer store.resourcesMux.Unlock()

	entry := r.resource
	entry.Unlock()
	r.resource = nil

	if entry.Release() {
		store.resources.Remove(r.key)
	}
}

func (r *Resource) Unmarshal(to interface{}) error {
	checkLocked(r.isLocked)

	err := func() error {
		r.resource.valueMu.Lock()
		defer r.resource.valueMu.Unlock()
		return r.initValue()
	}()
	if err != nil {
		return err
	}

	return typeconv.Convert(to, r.resource.value)
}

func (r *Resource) Update(val interface{}) error {
	checkLocked(r.isLocked)

	r.resource.valueMu.Lock()
	defer r.resource.valueMu.Unlock()

	if err := r.initValue(); err != nil {
		return err
	}
	return typeconv.Convert(&r.resource.value, val)
}

func (r *Resource) initValue() error {
	if r.resource.value != nil {
		return nil
	}

	err := r.store.persistentStore.View(func(tx *registry.Tx) error {
		vd, err := tx.Get(registry.Key(r.key))
		if err != nil {
			return err
		}

		r.resource.value = common.MapStr{}
		if vd == nil {
			return nil
		}
		return vd.Decode(&r.resource.value)
	})

	if err != nil {
		return errors.Wrapf(err,
			"failed to access the resource '%s' in the registry", r.key)
	}
	return nil
}

func (r *Resource) Registry() *RegistryResource {
	return (*RegistryResource)(r)
}

func (r *RegistryResource) Update(val interface{}) error {
	checkLocked(r.isLocked)

	return r.store.persistentStore.Update(func(tx *registry.Tx) error {
		return tx.Update(registry.Key(r.key), val)
	})
}

func (r *RegistryResource) CreateUpdateLog(val interface{}) (ResourceUpdateOp, error) {
	checkLocked(r.isLocked)

	panic("TODO")
}

func checkLocked(b bool) {
	if !b {
		panic("try to access unlocked resource")
	}
}
