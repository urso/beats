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

	"github.com/elastic/go-concert"
	"github.com/elastic/go-concert/unison"
)

// globalLockManager gives access to the LockManager. It ensures that the global lock is
// locked for as long as any session holds the lock.
type globalLockManager struct {
	prefix  string
	manager *unison.LockManager
}

// globalLock keeps track of lock/unlocks within shared sessions, ensuring that the managed lock
// from the global LockManager is kept for as long as at least one entry in the StateStore
// require the lock. The global lock will be freed after all session that do access the lock
// have been finished.
type globalLock struct {
	entry     *resourceEntry
	lock      *unison.ManagedLock
	session   *unison.LockSession
	mu        sync.Mutex
	lockCount int
}

type shadowLockSession struct {
	done     *concert.OnceSignaler
	unlocked *concert.OnceSignaler
	lockLost *concert.OnceSignaler
}

func newGlobalLockManager(name string, lockmngr *unison.LockManager) *globalLockManager {
	return &globalLockManager{
		prefix:  name,
		manager: lockmngr,
	}
}

func (glm *globalLockManager) Access(key string) *globalLock {
	if glm.prefix != "" {
		key = glm.prefix + "/" + key
	}
	return newGlobalLock(glm.manager.Access(key))
}

func newGlobalLock(lock *unison.ManagedLock) *globalLock {
	return &globalLock{lock: lock}
}

func (l *globalLock) Lock() *unison.LockSession {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.lockCount > 0 {
		l.lockCount++
		return l.session
	}

	l.session = l.lock.Lock(l.lockCallbackOpt())
	l.onLockAcquired()
	l.lockCount++
	return l.session
}

func (l *globalLock) TryLock() (*unison.LockSession, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.lockCount > 0 {
		l.lockCount++
		return l.session, true
	}

	session, success := l.lock.TryLock(l.lockCallbackOpt())
	if success {
		l.lockCount++
		l.session = session
		l.onLockAcquired()
	}
	return session, success
}

func (l *globalLock) Unlock() {
	l.mu.Lock()
	defer l.mu.Unlock()

	invariant(l.lockCount > 0, "attempting to unlock unlocked global store lock")

	l.lockCount--
	if l.lockCount == 0 {
		l.session = nil
		l.lock.Unlock()
	}
}

func (l *globalLock) lockCallbackOpt() unison.LockOption {
	return unison.WithSignalCallbacks{
		Done: l.onLockDone,
		Lost: l.onLockLost,
	}
}

func (l *globalLock) onLockAcquired() {
}

// onLockDone is called when the global lock provided by the LockManager is either released
// or we have lost the lock. onLockDone invalidated all state of pending update
// operations and the local cache status.
// When the global is acquired again, we ensure that the state is read from the registry again.
func (l *globalLock) onLockDone() {
	val := &l.entry.value
	val.mux.Lock()
	defer val.mux.Unlock()

	val.pendingIgnore += val.pendingGood
	val.pendingGood = 0
	val.cached = nil
}

func (l *globalLock) onLockLost() {
	entry := l.entry
	entry.muInternal.Lock()
	defer entry.muInternal.Unlock()
	if entry.lockSession != nil {
		entry.lockSession.signalLost()
	}
}

func newShadowLockSession() *shadowLockSession {
	return &shadowLockSession{
		done:     concert.NewOnceSignaler(),
		unlocked: concert.NewOnceSignaler(),
		lockLost: concert.NewOnceSignaler(),
	}
}

func (s *shadowLockSession) Done() <-chan struct{}     { return s.done.Done() }
func (s *shadowLockSession) Unlocked() <-chan struct{} { return s.unlocked.Done() }
func (s *shadowLockSession) LockLost() <-chan struct{} { return s.lockLost.Done() }

func (s *shadowLockSession) signalUnlocked() {
	s.unlocked.Trigger()
	s.done.Trigger()
}

func (s *shadowLockSession) signalLost() {
	s.lockLost.Trigger()
	s.done.Trigger()
}
