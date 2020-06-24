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
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/common/cleanup"
	"github.com/elastic/beats/v7/libbeat/logp"
)

type diskstore struct {
	log            *logp.Logger
	checkpointPred CheckpointPredicate

	home        string
	logFileName string
	dataFiles   []dataFileInfo

	txid uint64

	fileMode   os.FileMode
	bufferSize int
	logFile    *os.File
	logBuf     *bufio.Writer

	logFileSize      uint64
	logEntries       uint
	logInvalid       bool
	logNeedsTruncate bool
}

// dataFileInfo is used to track and sort on disk data files.
// We should have only one data file on disk, but in case delete operations
// have failed or not finished dataFileInfo is used to detect the ordering.
type dataFileInfo struct {
	path string
	txid uint64
}

// storeEntry is used to write entries to the checkpoint file only.
type storeEntry struct {
	Key    string        `struct:"_key"`
	Fields common.MapStr `struct:",inline"`
}

// storeMeta is read from the meta file.
type storeMeta struct {
	Version string `struct:"version"`
}

// logAction is prepended to each operation logged to the update file.
// It contains the update ID, a sequential counter to track correctness,
// and the action name.
type logAction struct {
	Op string `json:"op"`
	ID uint64 `json:"id"`
}

const (
	logFileName  = "log.json"
	metaFileName = "meta.json"

	storeVersion = "1"

	keyField = "_key"
)

// newDiskStorek initializes the disk store stucture only. The store must have
// been opened already.  It tries to open the update log file for append
// operations. If opening the update log file fails, it is marked as
// 'corrupted', triggering a checkpoint operation on the first update to the store.
func newDiskStore(
	log *logp.Logger,
	home string,
	dataFiles []dataFileInfo,
	txid uint64,
	mode os.FileMode,
	entries uint,
	logInvalid bool,
	bufferSize uint,
	checkpointPred CheckpointPredicate,
) *diskstore {
	s := &diskstore{
		log:              log.With("path", home),
		home:             home,
		logFileName:      filepath.Join(home, logFileName),
		dataFiles:        dataFiles,
		txid:             txid,
		fileMode:         mode,
		bufferSize:       int(bufferSize),
		logFile:          nil,
		logBuf:           nil,
		logEntries:       entries,
		logInvalid:       logInvalid,
		logNeedsTruncate: false, // only truncate on next checkpoint
		checkpointPred:   checkpointPred,
	}

	_ = s.tryOpenLog()
	return s
}

// tryOpenLog access the update log. The log file is truncated if a checkpoint operation has been
// executed last.
// The log file is marked as invalid if opening it failed. This will trigger a checkpoint operation
// and another call to tryOpenLog in the future.
func (s *diskstore) tryOpenLog() error {
	flags := os.O_RDWR | os.O_CREATE
	if s.logNeedsTruncate {
		flags |= os.O_TRUNC
	}

	f, err := os.OpenFile(s.logFileName, flags, s.fileMode)
	if err != nil {
		s.log.Errorf("Failed to open file %v: %v", s.logFileName, err)
		return err
	}

	ok := false
	defer cleanup.IfNot(&ok, func() {
		f.Close()
	})

	_, err = f.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}

	if s.logNeedsTruncate {
		s.logEntries = 0 // reset counter if file was truncated on Open
		s.logFileSize = 0
	} else {
		info, err := f.Stat()
		if err != nil {
			return err
		}

		s.logFileSize = uint64(info.Size())
	}

	ok = true
	s.logNeedsTruncate = false
	s.logFile = f
	s.logBuf = bufio.NewWriterSize(&ensureWriter{s.logFile}, s.bufferSize)
	return nil
}

// mustCheckpoint returns true if the store is required to execute a checkpoint
// operation, either by predicate or by some internal state detecting a problem
// with the log file.
func (s *diskstore) mustCheckpoint() bool {
	return s.logInvalid || s.checkpointPred(s.logFileSize)
}

func (s *diskstore) Close() error {
	if s.logFile != nil {
		// always sync log file on ordinary shutdown.
		err := s.logBuf.Flush()
		if err == nil {
			err = syncFile(s.logFile)
		}
		s.logFile.Close()
		s.logFile = nil
		s.logBuf = nil
		return err
	}
	return nil
}

// log operation adds another entry to the update log file.
// The log file is marked as invalid if the write fails. This will trigger a
// checkpoint operation in the future.
func (s *diskstore) LogOperation(op op) error {
	if s.logInvalid {
		return errLogInvalid
	}

	if s.logFile == nil {
		// We continue in case we have errors accessing the log file, but mark the
		// store as invalid. This will force a full state checkpoint.
		// The call to tryOpenLog prints some error log, we only use the error as
		// indicator to invalidate the disk store, so we can try to recover by
		// checkpointing.
		if err := s.tryOpenLog(); err != nil {
			s.logInvalid = true
			return err
		}
	}

	writer := s.logBuf
	counting := &countWriter{w: writer}
	enc := newJSONEncoder(counting)
	defer func() {
		s.logFileSize += counting.n
	}()

	ok := false
	defer cleanup.IfNot(&ok, func() {
		s.logInvalid = true
	})

	if err := enc.Encode(logAction{
		Op: op.name(),
		ID: s.txid + 1,
	}); err != nil {
		return err
	}
	writer.WriteByte('\n')

	if err := enc.Encode(op); err != nil {
		return err
	}
	writer.WriteByte('\n')

	if err := writer.Flush(); err != nil {
		return err
	}

	ok = true
	s.logEntries++
	s.txid++
	return nil
}

// WriteCheckpoint serializes all state into a json file. The file contains an
// array with all states known to the memory storage.
// WriteCheckpoint first serializes all state to a temporary file, and finally
// replaces move the temporary data file into the correct location. No files
// are overwritten or replaced. Instead the change sequence number is used for
// the filename, and older data files will be deleleted after success.
//
// The active marker file is overwritten after all updates did succeed. The
// marker file contains the filename of the current valid data-file.
// NOTE: due to limitation on some Operating system or file systems, the active
//       marker is not a symlink, but an actual file.
func (s *diskstore) WriteCheckpoint(state map[string]entry) error {
	tmpPath, err := s.checkpointTmpFile(filepath.Join(s.home, "checkpoint"), state)
	if err != nil {
		return err
	}

	fileTxID := s.txid + 1
	fileName := fmt.Sprintf("%v.json", fileTxID)
	checkpointPath := filepath.Join(s.home, fileName)

	if err := os.Rename(tmpPath, checkpointPath); err != nil {
		return err
	}
	trySyncPath(s.home)

	// clear transaction log once finished
	s.checkpointClearLog()

	// finish current on-disk transaction by increasing the txid
	s.txid++

	s.dataFiles = append(s.dataFiles, dataFileInfo{
		path: checkpointPath,
		txid: fileTxID,
	})

	// delete old transaction files
	active, _ := activeDataFile(s.dataFiles)
	updateActiveMarker(s.log, s.home, active)
	s.removeOldDataFiles()

	trySyncPath(s.home)
	return nil
}

func (s *diskstore) checkpointTmpFile(baseName string, states map[string]entry) (string, error) {
	tempfile := baseName + ".new"
	f, err := os.OpenFile(tempfile, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_SYNC, s.fileMode)
	if err != nil {
		return "", err
	}

	ok := false
	defer cleanup.IfNot(&ok, func() {
		f.Close()
	})

	writer := bufio.NewWriterSize(&ensureWriter{f}, s.bufferSize)
	enc := newJSONEncoder(writer)
	if _, err = writer.Write([]byte{'['}); err != nil {
		return "", err
	}

	first := true
	for key, entry := range states {
		prefix := []byte(",\n")
		if first {
			prefix = prefix[1:]
			first = false
		}
		if _, err = writer.Write(prefix); err != nil {
			return "", err
		}

		err = enc.Encode(storeEntry{
			Key:    key,
			Fields: entry.value,
		})
		if err != nil {
			return "", err
		}
	}

	if _, err = writer.Write([]byte("\n]")); err != nil {
		return "", err
	}

	if err = writer.Flush(); err != nil {
		return "", err
	}

	if err = syncFile(f); err != nil {
		return "", err
	}

	ok = true
	if err = f.Close(); err != nil {
		return "", err
	}

	return tempfile, nil
}

func (s *diskstore) checkpointClearLog() {
	if s.logFile == nil {
		s.logNeedsTruncate = true
		return
	}

	err := s.logFile.Truncate(0)
	if err == nil {
		_, err = s.logFile.Seek(0, os.SEEK_SET)
	}

	if err != nil {
		s.logFile.Close()
		s.logFile = nil
		s.logBuf = nil
		s.logNeedsTruncate = true
		s.logInvalid = true
	}

	s.logEntries = 0
	s.logFileSize = 0
}

func updateActiveMarker(log *logp.Logger, home, active string) error {
	activeLink := filepath.Join(home, "active.dat")
	tmpLink := filepath.Join(home, "active.dat")
	log = log.With("temporary", tmpLink, "data_file", active, "link_file", activeLink)

	if active == "" {
		if err := os.Remove(activeLink); err != nil { // try, remove active symlink if present.
			log.Errorf("Failed to remove old pointer file: %v", err)
		}
		return nil
	}

	// Atomically try to update the pointer file to the most recent data file.
	// We 'simulate' the atomic update by create the temporary active.json.tmp symlink file,
	// which we rename to active.json. If active.json.tmp exists we remove it.
	if err := os.Remove(tmpLink); err != nil && !os.IsNotExist(err) {
		log.Errorf("Failed to remove old temporary symlink file: %v", err)
		return err
	}
	if err := ioutil.WriteFile(tmpLink, []byte(active), 0600); err != nil {
		log.Errorf("Failed to write temporary pointer file: %v", err)
		return err
	}
	if err := os.Rename(tmpLink, activeLink); err != nil {
		log.Errorf("Failed to replace link file: %v", err)
		return err
	}

	trySyncPath(home)
	return nil
}

// removeOldDataFiles sorts the data files by their update sequence number and
// finally deletes all but the newest file from the storage directory.
func (s *diskstore) removeOldDataFiles() {
	L := len(s.dataFiles)
	if L <= 1 {
		return
	}

	removable, keep := s.dataFiles[:L-1], s.dataFiles[L-1:]
	for i := range removable {
		path := removable[i].path
		err := os.Remove(path)
		if err == nil || os.IsNotExist(err) {
			continue
		}

		// ohoh... stop removing and construct new array of leftover data files
		s.dataFiles = append(removable[i:], keep...)
		return
	}
	s.dataFiles = keep
}

// activeDataFile returns the most recent data file in a list of present (sorted)
// data files.
func activeDataFile(infos []dataFileInfo) (string, uint64) {
	if len(infos) == 0 {
		return "", 0
	}

	active := infos[len(infos)-1]
	return active.path, active.txid
}

// listDataFiles returns a sorted list of data files with txid per file.
// The list is sorted by txid, in ascending order (taking integer overflows
// into account).
func listDataFiles(home string) ([]dataFileInfo, error) {
	files, err := filepath.Glob(filepath.Join(home, "*.json"))
	if err != nil {
		return nil, err
	}

	var infos []dataFileInfo
	for i := range files {
		info, err := os.Lstat(files[i])
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() {
			continue
		}

		name := filepath.Base(files[i])
		name = name[:len(name)-5] // remove '.json' extension

		id, err := strconv.ParseUint(name, 10, 64)
		if err == nil {
			infos = append(infos, dataFileInfo{
				path: files[i],
				txid: id,
			})
		}
	}

	// empty or most recent snapshot was complete (old data file has been deleted)
	if len(infos) <= 1 {
		return infos, nil
	}

	// sort files by transaction ID
	sort.Slice(infos, func(i, j int) bool {
		return isTxIDLessEqual(infos[i].txid, infos[j].txid)
	})
	return infos, nil
}

// loadDataFile create a new hashtable with all key/value pairs found.
func loadDataFile(path string, tbl map[string]entry) error {
	if path == "" {
		return nil
	}

	err := readDataFile(path, func(key string, state common.MapStr) {
		tbl[key] = entry{value: state}
	})
	return err
}

func readDataFile(path string, fn func(string, common.MapStr)) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var states []map[string]interface{}
	dec := json.NewDecoder(f)
	if err := dec.Decode(&states); err != nil {
		return fmt.Errorf("corrupted data file: %v", err)
	}

	for _, state := range states {
		keyRaw := state["_key"]
		key, ok := keyRaw.(string)
		if !ok {
			continue
		}

		delete(state, "_key")
		fn(key, common.MapStr(state))
	}

	return nil
}

// loadLogFile applies all recorded transaction to an already initialized
// memStore.
// The txid is the transaction ID of the last known valid data file.
// Transactions older then txid will be ignored.
// loadLogFile returns the last commited txid in logTxid and the total number
// of operations in logCount.
// An incomplete transaction is recorded at the end of the log file, if
// complete is false.
func loadLogFile(
	store *memstore,
	txid uint64,
	home string,
) (logTxid uint64, entries uint, err error) {
	err = readLogFile(home, func(rawOp op, id uint64) error {
		// ignore old entries in case the log file truncation was not executed between a beat restart.
		if isTxIDLessEqual(id, txid) {
			return nil
		}

		if id != txid+1 {
			return errTxIDInvalid
		}
		txid = id

		switch op := rawOp.(type) {
		case *opSet:
			entries++
			store.Set(op.K, op.V)
		case *opRemove:
			entries++
			store.Remove(op.K)
		}
		return nil
	})
	if err != nil {
		return txid, entries, err
	}

	return txid, entries, err
}

// readLogFile iterates all operations found in the transaction log.
func readLogFile(home string, fn func(op, uint64) error) error {
	path := filepath.Join(home, logFileName)
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	for dec.More() {
		var act logAction
		if err := dec.Decode(&act); err != nil {
			return err
		}

		var op op
		switch act.Op {
		case opValSet:
			op = &opSet{}
		case opValRemove:
			op = &opRemove{}
		}

		if err := dec.Decode(op); err != nil {
			return err
		}

		if err := fn(op, act.ID); err != nil {
			return err
		}
	}

	return nil
}

func checkMeta(meta storeMeta) error {
	if meta.Version != storeVersion {
		return fmt.Errorf("store version %v not supported", meta.Version)
	}

	return nil
}

func writeMetaFile(home string, mode os.FileMode) error {
	path := filepath.Join(home, metaFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, mode)
	if err != nil {
		return err
	}

	ok := false
	defer cleanup.IfNot(&ok, func() {
		f.Close()
	})

	enc := newJSONEncoder(&ensureWriter{f})
	err = enc.Encode(storeMeta{
		Version: storeVersion,
	})
	if err != nil {
		return err
	}

	if err := syncFile(f); err != nil {
		return err
	}

	ok = true
	if err := f.Close(); err != nil {
		return err
	}

	trySyncPath(home)
	return nil
}

func readMetaFile(home string) (storeMeta, error) {
	var meta storeMeta
	path := filepath.Join(home, metaFileName)

	f, err := os.Open(path)
	if err != nil {
		return meta, err
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	if err := dec.Decode(&meta); err != nil {
		return meta, fmt.Errorf("can not read store meta file: %v", err)
	}

	return meta, nil
}

// isTxIDLessEqual compares two IDs by checking that their distance is < 2^63.
// It always returns true if
//  - a == b
//  - a < b (mod 2^63)
//  - b > a after an integer rollover that is still within the distance of <2^63-1
func isTxIDLessEqual(a, b uint64) bool {
	return int64(a-b) <= 0
}