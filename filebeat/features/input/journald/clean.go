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

package journald

import (
	"time"

	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/statestore"
	"github.com/elastic/go-concert/unison"
)

// cleaner removes finished entries from the registry file.
type cleaner struct {
	log *logp.Logger
}

// run starts a loop that tries to clean entries from the registry.
// The cleaner locks the store, such that no new states can be created
// during the cleanup phase. Only resources that are finished and whos TTL
// (clean_timeout setting) has expired will be removed.
//
// Resources are considered "Finished" if they do not have a current owner (active input), and
// if they have no pending updates that still need to be written to the registry file after associated
// events have been ACKed by the outputs.
// The event acquisition timestamp is used as reference to clean resources. If a resources was blocked
// for a long time, and the life time has been exhausted, then the resource will be removed immediately
// once the last event has been ACKed.
func (c *cleaner) run(canceler unison.Canceler, store *store, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	started := time.Now()

	for {
		select {
		case <-ticker.C:
			c.cleanup(started, store)
		case <-canceler.Done():
			return
		}
	}
}

// Cleanup looks for resources to remove and deletes these. `cleanup` receives
// the start timestamp of the cleaner as reference. If we have entries without
// updates in the registry, that are older than `started`, we will use `started
// + ttl` to decide if an entry will be removed. This way old entries are not
// removed immediately on startup if the Beat is down for a longer period of
// time.
func (c *cleaner) cleanup(started time.Time, store *store) {
	c.log.Debugf("Start store cleanup")
	defer c.log.Debugf("Done store cleanup")

	states := store.ephemeralStore
	states.mu.Lock()
	defer states.mu.Unlock()

	now := time.Now()

	// keys stores all resource keys to delete. The bool indicates if the key was
	// already written to the registry file, or if the resource is still in
	// memory only (e.g. due to IO errors).
	keys := map[string]bool{}
	numStored := 0

	for key, resource := range states.table {
		if !resource.Finished() {
			continue
		}

		ttl := resource.state.Internal.TTL
		reference := resource.state.Internal.Updated
		if started.After(reference) {
			reference = started
		}

		if reference.Add(ttl).After(now) {
			continue
		}

		keys[key] = resource.stored
		if resource.stored {
			numStored++
		}
	}

	if len(keys) == 0 {
		c.log.Debug("No entries to remove found")
		return
	}

	// try to delete entries from the registry and internal state storage
	if numStored > 0 {
		err := store.persistentStore.Update(func(tx *statestore.Tx) error {
			for k, stored := range keys {
				if !stored {
					continue
				}

				if err := tx.Remove(statestore.Key(k)); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			c.log.Errorf("Failed to remove entries from the registry: %+v", err)
			return
		}
	}

	for key := range keys {
		delete(states.table, key)
	}
}
