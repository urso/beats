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

import (
	"fmt"
	"sync"
)

type LockTable interface {
	Access(key ResourceKey) Lock
}

// type ResourceKey []byte

type Lock interface {
	Lock()
	TryLock() bool
	Unlock()
}

type lock struct {
	islocked bool

	table *table
	key   keyPair
}

type table struct {
	mu   sync.Mutex
	bins map[uint64]*bin
}

type bin struct {
	entries []entry
}

type entry struct {
	key  ResourceKey
	lock *resLock
}

type entryRef struct {
	table *table
	bin   *bin
	hash  uint64
	idx   int
}

type keyPair struct {
	hash uint64
	key  ResourceKey
}

type resLock struct {
	count int
	ch    chan struct{}
}

func (l *lock) Lock() {
	if l.islocked {
		panic("resource is already locked")
	}

	l.table.mu.Lock()
	entry := l.table.access(l.key)
	entry.lock.count++
	l.table.mu.Unlock()

	<-entry.lock.ch
	l.islocked = true
}

func (l *lock) TryLock() bool {
	if l.islocked {
		panic("resource is already locked")
	}

	l.table.mu.Lock()
	defer l.table.mu.Unlock()

	entry := l.table.access(l.key)
	select {
	case <-entry.lock.ch:
		l.islocked = true
		entry.lock.count++
		return true
	default:
		return false
	}
}

func (l *lock) Unlock() {
	if !l.islocked {
		panic("resource is not locked")
	}

	l.table.mu.Lock()
	defer l.table.mu.Unlock()

	ref := l.table.find(l.key)
	if !ref.valid() {
		panic(fmt.Errorf("lock table in inconsistent state when accessing: %v", l.key.key))
	}

	entry := ref.access()
	entry.lock.count--
	if entry.lock.count == 0 {
		ref.del()
	} else {
		entry.lock.ch <- struct{}{}
	}
}

func newTable() *table {
	return &table{
		bins: map[uint64]*bin{},
	}
}

func (t *table) Access(key ResourceKey) Lock {
	return &lock{
		islocked: false,
		table:    t,
		key: keyPair{
			hash: key.Hash(),
			key:  key,
		},
	}
}

func (t *table) access(k keyPair) *entry {
	ref := t.getOrCreate(k)
	return ref.access()
}

func (t *table) getOrCreate(k keyPair) entryRef {
	b := t.bins[k.hash]
	if b == nil {
		b = &bin{}
		t.bins[k.hash] = b
	}

	idx := b.find(k)
	if idx < 0 {
		idx = b.add(k)
	}

	return entryRef{t, b, k.hash, idx}
}

func (t *table) find(k keyPair) entryRef {
	bin := t.bins[k.hash]
	if bin == nil {
		return entryRef{idx: -1}
	}

	idx := bin.find(k)
	return entryRef{t, bin, k.hash, idx}
}

func (b *bin) find(k keyPair) int {
	for i := range b.entries {
		if k.key.Equal(b.entries[i].key) {
			return i
		}
	}

	return -1
}

func (b *bin) add(k keyPair) int {
	idx := len(b.entries)

	ch := make(chan struct{}, 1)
	ch <- struct{}{}

	b.entries = append(b.entries, entry{
		key: k.key,
		lock: &resLock{
			ch: ch,
		},
	})
	return idx
}

func (r *entryRef) valid() bool {
	return r.idx >= 0
}

func (r *entryRef) access() *entry {
	return &r.bin.entries[r.idx]
}

func (r *entryRef) del() {
	end := len(r.bin.entries) - 1
	r.bin.entries[r.idx] = r.bin.entries[end]
	r.bin.entries[end] = entry{}
	r.bin.entries = r.bin.entries[:end]

	if len(r.bin.entries) == 0 {
		delete(r.table.bins, r.hash)
	}
}
