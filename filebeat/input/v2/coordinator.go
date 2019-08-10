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

package v2

/*
type Resource struct {
	key     keyPair
	table   *table
	hasLock bool
}

type Key interface {
	Hash() uint64
	Equal(other interface{}) bool
	Serialize() []byte
}

type keyPair struct {
	hash uint64
	key  Key
}

type table struct {
	bins map[uint64]*bin
}

type bin struct {
	entries []entry
	free    []int
}

type entry struct {
	key Key
	idx int

	accessors int // number of active accessors
	pending   int // number of events that need to be ACKed

	state resourceState
	lock  chan struct{}
}

type resourceState int

const (
	resourceUnset resourceState = iota
	resourceAvail
	resourceLocked
)

func (r *Resource) Lock() bool {
	r.table.mu.Lock()

	entry := r.table.access(r.key)
	if entry.state != resourceLocked {
		entry.state = resourceLocked
	}
	r.table.mu.Unlock()
	entry.lock <- struct{}{}
}

func (r *Resource) TryLock() bool {
	r.table.mu.Lock()
	defer r.table.mu.Unlock()

	entry := r.table.access(r.key)
	select {
	case entry.lock <- struct{}{}:
		entry.state = resourceLocked
		return true
	default:
		entry.accessors--
		return false
	}
}

func (r *Resource) Unlock() {
	r.table.mu.Lock()
	defer r.table.mu.Unlock()

	entry := r.table.access(r.key)
	entry.state = resourceAvail
}

func (r *Resource) Update(val interface{}) error {
	panic("TODO")
}

func (r *Resource) Get() interface{} {
	panic("TODO")
}

func (r *Resource) AddPending() {
}

func (t *table) access(k keyPair) *entry {
	bin := t.bins[k.hash]
	idx := bin.find(k)
	if idx < 0 {
		idx = bin.add(k)
	}

	entry := &bin.entries[idx]
	entry.accessors++
	return entry
}

func (b *bin) find(k keyPair) int {
	for i := range b.entries {
		if b.entries[i].state == resourceUnset {
			continue
		}

		if k.key.Equal(b.entries[i].key) {
			return i
		}
	}

	return -1
}

func (b *bin) add(k keyPair) int {
	L := len(b.entries)

	idx := L
	if len(b.free) > 0 {
		end := len(b.free) - 1
		idx = b.free(end)
		b.free = b.free[:idx]
	} else {
		b.entries = append(b.entries, entry{})
	}

	b.entries[idx] = entry{
		key:   k,
		idx:   idx,
		state: resourceAvail,
		lock:  make(chan struct{}, 1),
	}
}

*/

/*

type Coordinator struct {
}

type Resource struct {
	table *table
	key   keyPair
}

type ResourceKey interface {
	Hash() uint64
	Equal(other interface{}) bool
}

type table struct {
	mu   sync.Mutex
	bins map[uint64]*bin
}

type bin struct {
	entries  []entry
	freelist []int
}

type entry struct {
	key   ResourceKey
	state resourceState

	lock     chan struct{}
	refCount int
}

type entryRef struct {
	bin *bin
	idx int
}

type keyPair struct {
	hash uint64
	key  ResourceKey
}

type resourceState int

const (
	resourceUnset resourceState = iota
	resourceAvail
	resourceLocked
)

func (r *Resource) Lock() {
	r.table.mu.Lock()

	ref := r.table.find(r.key)
	if ref.idx >= 0 {
		entry := ref.access()
		entry.refCount++
		if entry.state == resourceLocked {
			r.table.mu.Unlock()
			<-entry.lock
		} else {
			entry.state = resourceLocked
		}
		return
	}
}

func (r *Resource) Unlock() {
}

func (r *Resource) TryLock() {
}

func (r *Resource) AddPending() {
}

func (r *Resource) Update(info interface{}) error {
	panic("TODO")
}

func (t *table) init() {
	t.bins = map[uint64]*bin{}
}

func (t *table) find(k keyPair) entryRef {
	bin := t.bins[k.hash]
	return entryRef{
		bin: bin,
		idx: bin.index(k),
	}
}

func (t *table) add(k keyPair) entryRef {
	bin := t.bins[k.hash]
	idx := bin.add(k)
	return entryRef{
		bin: bin,
		idx: idx,
	}
}

func (b *bin) index(k keyPair) int {
	for i := range b.entries {
		entry := &b.entries[i]

		if entry.state == resourceUnset {
			continue
		}

		if k.key.Equal(entry.key) {
			return i
		}
	}
	return -1
}

func (b *bin) add(k keyPair) int {
	i := len(b.entries)
	b.entries = append(b.entries, entry{
		key:      k,
		state:    resourceAvail,
		lock:     lock,
		refCount: 1,
	})
	return i
}

func (ref entryRef) access() *entry {
	if ref.idx < 0 {
		return nil
	}
	return &ref.bin.entries[ref.idx]
}

*/
