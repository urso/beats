package memlog2

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/common/cleanup"
	"github.com/elastic/beats/v7/libbeat/logp"
)

type diskstore struct {
	log           *logp.Logger
	checkpontPred CheckpointPredicate

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

type storeEntry struct {
	Key    string        `struct:"_key"`
	Fields common.MapStr `struct:",inline"`
}

type dataFileInfo struct {
	path string
	txid uint64
}

type storeMeta struct {
	Version string `struct:"version"`
}

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

func newDiskStore(
	log *logp.Logger,
	home string,
	dataFiles []dataFileInfo,
	txid uint64,
	mode os.FileMode,
	entries uint,
	logInvalid bool,
	bufferSize uint,
	checkpontPred CheckpointPredicate,
) *diskstore {
	s := &diskstore{
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
		checkpontPred:    checkpontPred,
	}

	_ = s.tryOpenLog()
	return s
}

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

func (s *diskstore) mustCheckpoint() bool {
	return s.logInvalid || s.checkpontPred(s.logFileSize)
}

func (s *diskstore) Close() error {
	if s.logFile != nil {
		err := s.logFile.Close()
		s.logFile = nil
		s.logBuf = nil
		return err
	}
	return nil
}

func (s *diskstore) LogOperation(op op) error {
	return s.addOperation(op)
}

func (s *diskstore) addOperation(op op) error {
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
	enc := json.NewEncoder(counting)
	defer func() {
		s.logFileSize += counting.n
	}()

	ok := false
	defer cleanup.IfNot(&ok, func() {
		s.logInvalid = true
	})

	if err := encOp(enc, op, s.txid+1); err != nil {
		return err
	}
	if err := writer.Flush(); err != nil {
		return err
	}

	ok = true
	s.logEntries++
	s.txid++
	return nil
}

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
	s.syncHome()

	// clear transaction log once finished
	s.checkpointClearLog()

	// finish current on-disk transaction by increasing the txid
	s.txid++

	s.dataFiles = append(s.dataFiles, dataFileInfo{
		path: checkpointPath,
		txid: fileTxID,
	})

	// delete old transaction files
	if err := s.updateActiveSymLink(); err == nil {
		s.removeOldDataFiles()
	}

	s.syncHome()
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

func (s *diskstore) syncHome() {
	trySyncPath(s.home)
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
}

func (s *diskstore) updateActiveSymLink() error {
	activeLink := filepath.Join(s.home, "active.json")

	active, _ := activeDataFile(s.dataFiles)
	if active == "" {
		os.Remove(activeLink) // try, remove active symlink if present.
		return nil
	}

	active = filepath.Base(active)
	tmpLink := filepath.Join(s.home, "active.json.tmp")
	if err := os.Symlink(active, tmpLink); err != nil {
		return err
	}

	err := os.Rename(tmpLink, activeLink)
	if err != nil {
		return err
	}

	s.syncHome()
	return nil
}

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

func encOp(enc *json.Encoder, op op, id uint64) error {
	if err := enc.Encode(logAction{
		Op: op.name(),
		ID: id,
	}); err != nil {
		return err
	}
	return enc.Encode(op)
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
		idI := infos[i].txid
		idJ := infos[j].txid
		return int64(idI-idJ) < 0 // check idI < idJ (ids can overflow)
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

	var states []storeEntry
	dec := json.NewDecoder(f)
	if err := dec.Decode(&states); err != nil {
		return fmt.Errorf("corrupted data file: %v", err)
	}

	for _, state := range states {
		fn(state.Key, state.Fields)
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
		if id != txid+1 {
			return errTxIDInvalid
		}
		txid = id

		switch op := rawOp.(type) {
		case *opSet:
			entries++
			store.Set(op.K, op.V)
			/*
				case *opUpdate:
					entries++
					store.Update(op.K, op.V)
			*/
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
			/*
				case opValUpdate:
					op = &opUpdate{}
			*/
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
