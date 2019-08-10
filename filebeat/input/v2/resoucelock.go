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

import "sync"

type Lock struct {
	islocked bool

	table *table
	key   keyPair
	lock  *resLock
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
	entries []entry
}

type entry struct {
	key  ResourceKey
	lock *resLock
}

type keyPair struct {
	hash uint64
	key  ResourceKey
}

type resLock struct {
	count int
	ch    chan struct{}
}

func (l *Lock) Lock() {
	if l.islocked {
		panic("resource is already locked")
	}

	l.table.mu.Lock()
	entry := t.access(l.key)
	lock := entry.lock
	lock.count++
	l.table.mu.Unlock()

	<-lock.ch
	l.islocked = true
}

func (l *Lock) TryLock() bool {
	if l.islocked {
		panic("resource is already locked")
	}

	l.table.mu.Lock()
	defer l.table.mu.Unlock()

	entry := t.access(l.key)
	lock
	select {
	case <-lock.ch:
		l.islocked = true
		lock.count++
		return true
	default:
		return false
	}
}

func (l *Lock) Unlock() {
	if !l.islocked {
		panic("resource is not locked")
	}

	l.table.mu.Lock()
	defer l.table.mu.Unlock()
}

func (t *table) access(k keyPair) *entry {
	bin := t.bins[k.hash]
	idx := bin.find(k)
	if idx < 0 {
		idx = bin.add(k)
	}

	entry := &bin.entries[idx]
	return entry
}

func (b *table) find(k keyPair) *entry {
	bin := t.bins[k.hash]
	idx := b.find(k)
	return &bin.entries[idx]
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
