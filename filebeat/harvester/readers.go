package harvester

import (
	"io"
	"time"
)

type lineReader interface {
	Next() ([]byte, int, error)
}

// noEOLLineReader strips last EOL from input line reader (if present).
type noEOLLineReader struct {
	reader lineReader
}

// timedReader keeps track of last time bytes have been read from underlying
// reader.
type timedReader struct {
	reader       io.Reader
	lastReadTime time.Time // last time we read some data from input stream
}

func newNoEOLLineReader(r lineReader) *noEOLLineReader {
	return &noEOLLineReader{r}
}

func (n *noEOLLineReader) Next() ([]byte, int, error) {
	line, size, err := n.reader.Next()
	if err != nil {
		return line, size, err
	}

	return line[:len(line)-lineEndingChars(line)], size, err
}

func newTimedReader(reader io.Reader) *timedReader {
	r := &timedReader{
		reader: reader,
	}
	return r
}

func (r *timedReader) Read(p []byte) (int, error) {
	var err error
	n := 0

	for i := maxConsecutiveEmptyReads; i > 0; i-- {
		n, err = r.reader.Read(p)
		if n > 0 {
			r.lastReadTime = time.Now()
			break
		}

		if err != nil {
			break
		}
	}

	return n, err
}
