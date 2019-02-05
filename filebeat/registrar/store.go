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

package registrar

import (
	"os"
	"sync"
	"time"

	"github.com/elastic/beats/filebeat/config"
	"github.com/elastic/beats/filebeat/input/file"
	"github.com/elastic/beats/libbeat/kvstore"
	"github.com/elastic/beats/libbeat/kvstore/backend/memlog"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/monitoring"
	"github.com/elastic/beats/libbeat/paths"
)

type Registrar struct {
	Channel chan []file.State
	out     successLogger

	// shutdown handling
	done chan struct{}
	wg   sync.WaitGroup

	kvReg *kvstore.Registry
	store *kvstore.Store

	gcRequired bool // gcRequired is set if registry state needs to be gc'ed before the next write
	gcEnabled  bool // gcEnabled indicates the registry contains some state that can be gc'ed in the future

	mux            sync.RWMutex
	managed        map[string]activityState // per file activity state not to be stored on disk
	pendingUpdates int
	pending        stateTable
	flushTimeout   time.Duration
}

type successLogger interface {
	Published(n int) bool
}

type activityState struct {
	Finished  bool
	FileInfo  os.FileInfo
	Timestamp time.Time
	TTL       time.Duration
}

var (
	statesUpdate    = monitoring.NewInt(nil, "registrar.states.update")
	statesCleanup   = monitoring.NewInt(nil, "registrar.states.cleanup")
	statesCurrent   = monitoring.NewInt(nil, "registrar.states.current")
	registryWrites  = monitoring.NewInt(nil, "registrar.writes.total")
	registryFails   = monitoring.NewInt(nil, "registrar.writes.fail")
	registrySuccess = monitoring.NewInt(nil, "registrar.writes.success")
)

func New(cfg config.Registry, out successLogger) (*Registrar, error) {
	home := paths.Resolve(paths.Data, cfg.Path)
	/*
		migrateFile := cfg.MigrateFile
		if migrateFile != "" {
			migrateFile = paths.Resolve(paths.Data, migrateFile)
		}

		err := ensureCurrent(home, migrateFile, cfg.Permissions)
		if err != nil {
			return nil, err
		}
	*/

	regBackend, err := memlog.New(memlog.Settings{
		Root:       home,
		FileMode:   cfg.Permissions,
		Checkpoint: nil,
	})
	if err != nil {
		return nil, err
	}

	reg := kvstore.NewRegistry(regBackend)
	store, err := reg.Get("filebeat")
	if err != nil {
		return nil, err
	}

	r := &Registrar{
		Channel: make(chan []file.State, 1),
		out:     out,
		done:    make(chan struct{}),
		kvReg:   reg,
		store:   store,
	}
	if err := r.Init(); err != nil {
		store.Close()
		reg.Close()
		return nil, err
	}

	return r, nil
}

func (r *Registrar) Init() error {
	managed := map[string]activityState{}
	err := r.store.View(func(tx *kvstore.Tx) error {
		return tx.Each(func(k kvstore.Key, vs kvstore.ValueDecoder) (bool, error) {
			/*
				state := struct {
					Timestamp time.Time `struct:"-"`
				}{}
				if err := vs.Decode(&state); err != nil {
					return false, fmt.Errorf("Failed to parse state (key=%v): %+v", k.String(), err)
				}
			*/

			state := struct {
				Timestamp time.Time `struct:"-"`
			}{}

			managed[k.String()] = activityState{
				Finished:  true,
				Timestamp: state.Timestamp,
				TTL:       -2,
			}
			return true, nil
		})
	})
	if err != nil {
		return err
	}

	r.managed = managed
	return nil
}

// GetStates returns the registrar states
func (r *Registrar) GetStates() ([]file.State, error) {
	r.mux.RLock()
	defer r.mux.RUnlock()

	var states []file.State
	err := r.store.View(func(tx *kvstore.Tx) error {
		return tx.Each(func(k kvstore.Key, vd kvstore.ValueDecoder) (bool, error) {
			if st, exist := r.pending[k.String()]; exist {
				states = append(states, st)
			} else {
				var state file.State
				if err := vd.Decode(&state); err == nil {
					activity, exist := r.managed[k.String()]
					if !exist {
						activity.Finished = true
						activity.TTL = -2
					}

					state.Finished = activity.Finished
					state.Fileinfo = activity.FileInfo
					state.Timestamp = activity.Timestamp
					state.TTL = activity.TTL
					states = append(states, state)
				} else {
					logp.Debug("registrar", "failed to decode state: %+v", err)
				}
			}
			return true, nil
		})
	})

	for _, state := range states {
		logp.Debug("registrar", "state read: %v", state)
	}

	return states, err
}

func (r *Registrar) Start() error {
	r.wg.Add(1)
	go r.Run()
	return nil
}

// Stop stops the registry. It waits until Run function finished.
func (r *Registrar) Stop() {
	logp.Info("Stopping Registrar")
	close(r.done)
	r.wg.Wait()

	r.store.Close()
	r.kvReg.Close()
}

func (r *Registrar) Run() {
	// Writes registry on shutdown
	defer func() {
		r.writeRegistry()
		r.wg.Done()
	}()

	var (
		timer  *time.Timer
		flushC <-chan time.Time
	)

	r.store.View(func(tx *kvstore.Tx) error {
		count, err := tx.Count()
		if err != nil {
			statesCurrent.Set(int64(count))
		}
		return nil
	})

	for {
		select {
		case <-r.done:
			logp.Info("Ending Registrar")
			return
		case <-flushC:
			flushC = nil
			timer.Stop()
			r.flushRegistry()
		case states := <-r.Channel:
			r.onEvents(states)
			if r.flushTimeout <= 0 {
				r.flushRegistry()
			} else if flushC == nil {
				timer = time.NewTimer(r.flushTimeout)
				flushC = timer.C
			}

		}
	}
}

func (r *Registrar) onEvents(states []file.State) {
	if len(states) > 0 {
		r.processStates(states)
		r.pendingUpdates += len(states)
	}

	// check if we need to enable state cleanup
	if !r.gcEnabled {
		for i := range states {
			if states[i].TTL >= 0 || states[i].Finished {
				r.gcEnabled = true
				break
			}
		}
	}

	logp.Debug("registrar", "Registrar state updates processed. Count: %v", len(states))

	// new set of events received -> mark state registry ready for next
	// cleanup phase in case gc'able events are stored in the registry.
	r.gcRequired = r.gcEnabled
}

func (r *Registrar) processStates(states []file.State) {
	r.mux.Lock()
	defer r.mux.Unlock()

	if r.pending == nil {
		r.pending = stateTable{}
	}

	ts := time.Now()
	for _, st := range states {
		st.Timestamp = ts

		k := st.ID()
		r.pending[k] = st
		r.managed[k] = activityState{
			Finished:  st.Finished,
			FileInfo:  st.Fileinfo,
			Timestamp: st.Timestamp,
			TTL:       st.TTL,
		}

		statesUpdate.Add(1)
	}
}

func (r *Registrar) flushRegistry() {
	if err := r.writeRegistry(); err != nil {
		logp.Err("Writing of registry returned error: %v. Continuing...", err)
	}

	if r.out != nil {
		r.out.Published(r.pendingUpdates)
	}
	r.pendingUpdates = 0
}

func (r *Registrar) writeRegistry() error {
	registryWrites.Inc()

	r.mux.Lock()
	defer r.mux.Unlock()

	err := r.store.Update(func(tx *kvstore.Tx) error {
		// First clean up states
		if err := r.gcStates(tx); err != nil {
			return err
		}

		for id, state := range r.pending {
			if err := tx.Set(kvstore.Key(id), state); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		registryFails.Inc()
	} else {
		registrySuccess.Inc()
	}

	return err
}

// gcStates runs a registry Cleanup. The method check if more event in the
// registry can be gc'ed in the future. If no potential removable state is found,
// the gcEnabled flag is set to false, indicating the current registrar state being
// stable. New registry update events can re-enable state gc'ing.
func (r *Registrar) gcStates(tx *kvstore.Tx) error {
	if !r.gcRequired {
		return nil
	}

	now := time.Now()

	numBefore, err := tx.Count()
	if err != nil {
		return err
	}

	// list of entries to be removed
	pendingClean := 0
	for k, st := range r.managed {
		if r.pending.Has(k) {
			continue
		}

		canExpire := st.TTL > 0
		expired := st.TTL == 0 || (canExpire && (now.Sub(st.Timestamp) > st.TTL))
		if expired && !st.Finished {
			logp.Err("State for %s should have been dropped, but couldn't as state is not finished.", k)
			expired = false
		}

		if expired {
			if err := tx.Remove(kvstore.Key(k)); err != nil {
				return err
			}
			delete(r.managed, k)
			logp.Debug("state", "State removed for '%s' because of older: %v", k, st.TTL)
		} else if canExpire || st.TTL == 0 {
			pendingClean++
		}
	}

	numAfter, err := tx.Count()
	if err != nil {
		return err
	}

	cleanedStates := numAfter - numBefore
	statesCleanup.Add(int64(cleanedStates))

	logp.Debug("registrar",
		"Registrar states cleaned up. Before: %d, After: %d, Pending: %d",
		numBefore, numAfter, pendingClean)

	r.gcRequired = false
	r.gcEnabled = pendingClean > 0
	statesCurrent.Set(int64(numAfter))
	return nil
}
