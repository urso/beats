package memlog2

import (
	"io"
	"os"
	"syscall"
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
