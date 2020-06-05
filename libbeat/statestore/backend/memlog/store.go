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
	return nil
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
	s.lock.Lock()
	defer s.lock.Unlock()

	var tmp common.MapStr
	if err := typeconv.Convert(&tmp, value); err != nil {
		return err
	}

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
	err := s.disk.LogOperation(op)
	if s.disk.mustCheckpoint() {
		checkpointErr := s.disk.WriteCheckpoint(s.mem.table)
		if err != nil {
			// report the checkpoint error if we don't have an up to date log file.
			// otherwise ignore the error, as the on disk state is known to be correct and
			// we can continue with a valid state after restart anyways.
			// The file will continue to be in 'invalid' state and the store tries to
			// checkpoint again on the next update.
			err = checkpointErr
		}
	}
	return err
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
