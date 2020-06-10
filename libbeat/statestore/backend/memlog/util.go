package memlog

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"syscall"

	"github.com/elastic/beats/v7/libbeat/logp"
)

type ensureWriter struct {
	w io.Writer
}

type countWriter struct {
	n uint64
	w io.Writer
}

func (c *countWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += uint64(n)
	return n, err
}

func (e *ensureWriter) Write(p []byte) (int, error) {
	var N int
	for len(p) > 0 {
		n, err := e.w.Write(p)
		N, p = N+n, p[n:]
		if isRetryErr(err) {
			return N, err
		}
	}
	return N, nil
}

func isRetryErr(err error) bool {
	return err == syscall.EINTR || err == syscall.EAGAIN
}

func trySyncPath(path string) {
	// best-effort fsync on path (directory). The fsync is required by some
	// filesystems, so to update the parents directory metadata to actually
	// contain the new file being rotated in.
	f, err := os.Open(path)
	if err != nil {
		return // ignore error, sync on dir must not be necessarily supported by the FS
	}
	defer f.Close()
	syncFile(f)
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
