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
	pendingUpdates int
	pending        stateTable
	flushTimeout   time.Duration
}

type successLogger interface {
	Published(n int) bool
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
					states = append(states, state)
				}
			}
			return true, nil
		})
	})

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
		r.pending[st.ID()] = st
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
	var remove []kvstore.Key
	pendingClean := 0
	err = tx.Each(func(k kvstore.Key, vs kvstore.ValueDecoder) (bool, error) {
		if r.pending.Has(k.String()) {
			return true, nil
		}

		value := struct {
			Finished  bool
			Timestamp time.Time     `json:"timestamp"`
			TTL       time.Duration `json:"ttl"`
		}{}
		if err := vs.Decode(&value); err != nil {
			logp.Err("Failed to decode registry entry '%s', removing.", k)
			remove = append(remove, k)
		}

		canExpire := value.TTL > 0
		expired := value.TTL == 0 || (canExpire && (now.Sub(value.Timestamp) > value.TTL))
		if expired && !value.Finished {
			logp.Err("State for %s should have been dropped, but couldn't as state is not finished.", k)
			expired = false
		}

		if expired {
			remove = append(remove, k)
			logp.Debug("state", "State removed for '%s' because of older: %v", k, value.TTL)
		} else if canExpire || value.TTL == 0 {
			pendingClean++
		}

		return true, nil
	})
	if err != nil {
		return err
	}

	for _, k := range remove {
		if err := tx.Remove(k); err != nil {
			return err
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
