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

package memlog

import (
	"fmt"
	"os"
	"sync"

	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/common/transform/typeconv"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/statestore/backend"
)

type store struct {
	lock sync.RWMutex
	disk *diskstore
	mem  memstore
}

type memstore struct {
	table map[string]entry
}

type entry struct {
	value common.MapStr
}

func openStore(log *logp.Logger, home string, mode os.FileMode, bufSz uint, checkpoint CheckpointPredicate) (*store, error) {
	fi, err := os.Stat(home)
	if os.IsNotExist(err) {
		err = os.MkdirAll(home, os.ModeDir|0770)
		if err != nil {
			return nil, err
		}

		err = writeMetaFile(home, mode)
		if err != nil {
			return nil, err
		}
	} else if !fi.Mode().IsDir() {
		return nil, fmt.Errorf("'%v' is no directory", home)
	}

	meta, err := readMetaFile(home)
	if err != nil {
		return nil, err
	}

	if err := checkMeta(meta); err != nil {
		return nil, err
	}

	dataFiles, err := listDataFiles(home)
	if err != nil {
		return nil, err
	}

	active, txid := activeDataFile(dataFiles)
	tbl := map[string]entry{}
	if err := loadDataFile(active, tbl); err != nil {
		return nil, err
	}

	logp.Info("Loading data file of '%v' succeeded. Active transaction id=%v", home, txid)

	var entries uint
	memstore := memstore{tbl}
	txid, entries, err = loadLogFile(&memstore, txid, home)
	logp.Info("Finished loading transaction log file for '%v'. Active transaction id=%v", home, txid)

	if err != nil {
		// Error indicates the log file was incomplete or corrupted.
		// Anyways, we already have the table in a valid state and will
		// continue opening the store from here.
		logp.Warn("Incomplete or corrupted log file in %v. Continue with last known complete and consistend state. Reason: %v", home, err)
	}

	diskstore := newDiskStore(log, home, dataFiles, txid, mode, entries, err != nil, bufSz, checkpoint)

	return &store{
		disk: diskstore,
		mem:  memstore,
	}, nil
}

func (s *store) Close() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.mem = memstore{}
	return s.disk.Close()
}

func (s *store) Has(key string) (bool, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.mem.Has(key), nil
}

func (s *store) Get(key string, into interface{}) error {
	s.lock.RLock()
	defer s.lock.RUnlock()

	dec := s.mem.Get(key)
	if dec == nil {
		return errKeyUnknown
	}
	return dec.Decode(into)
}

func (s *store) Set(key string, value interface{}) error {
	var tmp common.MapStr
	if err := typeconv.Convert(&tmp, value); err != nil {
		return err
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	s.mem.Set(key, tmp)
	return s.logOperation(&opSet{K: key, V: tmp})
}

func (s *store) Remove(key string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.mem.Remove(key)
	return s.logOperation(&opRemove{K: key})
}

func (s *store) logOperation(op op) error {
	if s.disk.mustCheckpoint() {
		err := s.disk.WriteCheckpoint(s.mem.table)
		if err != nil {
			// if writing the new checkpoint file failed we try to fallback to
			// appending the log operation.
			// TODO: make append configurable and retry checkpointing with backoff.
			s.disk.LogOperation(op)
		}

		return err
	}

	return s.disk.LogOperation(op)
}

func (s *store) Each(fn func(string, backend.ValueDecoder) (bool, error)) error {
	s.lock.RLock()
	defer s.lock.RUnlock()

	for k, entry := range s.mem.table {
		cont, err := fn(k, entry)
		if !cont || err != nil {
			return err
		}
	}

	return nil
}

func (m *memstore) Has(key string) bool {
	_, exists := m.table[key]
	return exists
}

func (m *memstore) Get(key string) backend.ValueDecoder {
	entry, exists := m.table[key]
	if !exists {
		return nil
	}
	return entry
}

func (m *memstore) Set(key string, value common.MapStr) {
	m.table[key] = entry{value: value}
}

func (m *memstore) Remove(key string) bool {
	_, exists := m.table[key]
	if !exists {
		return false
	}
	delete(m.table, key)
	return true
}

func (e entry) Decode(to interface{}) error {
	return typeconv.Convert(to, e.value)
}
