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
	"github.com/elastic/go-concert/atomic"
)

type table map[ResourceKey]*resource

type resource struct {
	key  ResourceKey
	lock chan struct{}

	refCount atomic.Uint

	valueMu sync.Mutex
	value   common.MapStr
}

func (t table) GetOrCreate(k ResourceKey) *resource {
	r := t.Find(k)
	if r == nil {
		lock := make(chan struct{}, 1)
		lock <- struct{}{}
		r = &resource{
			key:      k,
			lock:     lock,
			refCount: atomic.MakeUint(1),
		}
		t[k] = r
	} else {
		r.Retain()
	}

	return r
}

func (t table) Find(k ResourceKey) *resource {
	res := t[k]
	if res != nil {
		res.refCount.Inc()
	}
	return res
}

func (t table) Remove(k ResourceKey) {
	delete(t, k)
}

func (r *resource) Retain() {
	r.refCount.Inc()
}

func (r *resource) Release() bool {
	return r.refCount.Dec() == 0
}

func (r *resource) Lock() {
	<-r.lock
}

func (r *resource) TryLock() bool {
	select {
	case <-r.lock:
		return true
	default:
		return false
	}
}

func (r *resource) Unlock() {
	r.lock <- struct{}{}
}
