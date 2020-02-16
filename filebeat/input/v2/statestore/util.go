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
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/elastic/go-concert/unison"
)

type lockMode int

type channelCtx <-chan struct{}

const (
	lockRequired lockMode = iota
	lockAlreadyTaken
	lockMustRelease
)

func (ch channelCtx) Done() <-chan struct{} { return ch }
func (ch channelCtx) Err() error {
	select {
	case <-ch:
		return context.Canceled
	default:
		return nil
	}
}

func withLockMode(mux *sync.Mutex, lm lockMode, fn func() error) error {
	switch lm {
	case lockRequired:
		mux.Lock()
		defer mux.Unlock()
	case lockMustRelease:
		defer mux.Unlock()
	case lockAlreadyTaken:
	default:
		panic(fmt.Errorf("unknown lock mode: %v", lm))
	}

	return fn()
}

func ifNotOK(b *bool, fn func()) {
	if !(*b) {
		fn()
	}
}

func checkLocked(b bool) {
	invariant(!b, "try to access unlocked resource")
}

func checkNotLocked(b bool) {
	invariant(b, "invalid operation on locked resource")
}

func invariant(b bool, message string) {
	if !b {
		panic(errors.New(message))
	}
}

func sessionHoldsLock(session *unison.LockSession) bool {
	if session == nil {
		return false
	}

	select {
	case <-session.Done():
		return false
	default:
		return true
	}
}
