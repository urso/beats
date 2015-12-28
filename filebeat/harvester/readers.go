package harvester

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/elastic/beats/filebeat/harvester/encoding"
	"github.com/elastic/beats/filebeat/input"
	"github.com/elastic/beats/libbeat/logp"
)

type line struct {
	ts      time.Time
	content []byte
	sz      int
}

type lineReader interface {
	Next() (line, error)
}

type encLineReader struct {
	reader *encoding.LineReader
}

// noEOLLineReader strips last EOL from input line reader (if present).
type noEOLLineReader struct {
	reader lineReader
}

type logFileReader struct {
	fs        FileSource
	offset    int64
	config    logFileReaderConfig
	truncated bool

	lastTimeRead time.Time
	backoff      time.Duration
}

type boundedLineReader struct {
	reader   lineReader
	maxBytes int
}

type timeoutLineReader struct {
	reader  lineReader
	timeout time.Duration
	signal  error

	running bool
	ch      chan lineMessage
}

type lineMessage struct {
	line line
	err  error
}

type logFileReaderConfig struct {
	forceClose         bool
	maxInactive        time.Duration
	backoffDuration    time.Duration
	maxBackoffDuration time.Duration
	backoffFactor      int
}

var (
	errFileTruncate = errors.New("detected file being truncated")
	errForceClose   = errors.New("file must be closed")
	errInactive     = errors.New("file inactive")
	errTimeout      = errors.New("timeout")
)

func newEncLineReader(
	in io.Reader,
	codec encoding.Encoding,
	bufferSize int,
) (encLineReader, error) {
	r, err := encoding.NewLineReader(in, codec, bufferSize)
	return encLineReader{r}, err
}

func (r encLineReader) Next() (line, error) {
	c, sz, err := r.reader.Next()
	return line{ts: time.Now(), content: c, sz: sz}, err
}

func newNoEOLLineReader(r lineReader) *noEOLLineReader {
	return &noEOLLineReader{r}
}

func (n *noEOLLineReader) Next() (line, error) {
	line, err := n.reader.Next()
	if err != nil {
		return line, err
	}

	L := line.content
	line.content = L[:len(L)-lineEndingChars(L)]
	return line, err
}

func newLogFileReader(
	fs FileSource,
	config logFileReaderConfig,
) (*logFileReader, error) {
	var offset int64
	if seeker, ok := fs.(io.Seeker); ok {
		var err error
		offset, err = seeker.Seek(0, os.SEEK_CUR)
		if err != nil {
			return nil, err
		}
	}

	return &logFileReader{
		fs:           fs,
		offset:       offset,
		config:       config,
		lastTimeRead: time.Now(),
		backoff:      config.backoffDuration,
	}, nil
}

func (r *logFileReader) Read(buf []byte) (int, error) {
	fmt.Println("call Read")

	if r.truncated {
		var offset int64
		if seeker, ok := r.fs.(io.Seeker); ok {
			var err error
			offset, err = seeker.Seek(0, os.SEEK_CUR)
			if err != nil {
				return 0, err
			}
		}
		r.offset = offset
		r.truncated = false
	}

	for {
		n, err := r.fs.Read(buf)
		if n > 0 {
			fmt.Printf("did read(%v): '%s'\n", n, buf[:n])

			r.offset += int64(n)
			r.lastTimeRead = time.Now()
		}
		if err == nil {
			// reset backoff
			r.backoff = r.config.backoffDuration
			fmt.Printf("return size: %v\n", n)
			return n, nil
		}

		continuable := r.fs.Continuable()
		fmt.Printf("error: %v, continuable: %v\n", err, continuable)

		if err == io.EOF && !continuable {
			logp.Info("Reached end of file: %s", r.fs.Name())
			return n, err
		}

		if err != io.EOF || !continuable {
			logp.Err("Unexpected state reading from %s; error: %s", r.fs.Name(), err)
			return n, err
		}

		// Refetch fileinfo to check if the file was truncated or disappeared.
		// Errors if the file was removed/rotated after reading and before
		// calling the stat function
		info, statErr := r.fs.Stat()
		if statErr != nil {
			logp.Err("Unexpected error reading from %s; error: %s", r.fs.Name(), statErr)
			return n, statErr
		}

		// handle fails if file was truncated
		if info.Size() < r.offset {
			logp.Debug("harvester",
				"File was truncated as offset (%s) > size (%s). Begin reading file from offset 0: %s",
				r.offset, info.Size(), r.fs.Name())
			r.truncated = true
			return n, errFileTruncate
		}

		age := time.Since(r.lastTimeRead)
		if age > r.config.maxInactive {
			// If the file hasn't change for longer then maxInactive, harvester stops
			// and file handle will be closed.
			return n, errInactive
		}

		if r.config.forceClose {
			// Check if the file name exists (see #93)
			_, statErr := os.Stat(r.fs.Name())

			// Error means file does not exist. If no error, check if same file. If
			// not close as rotated.
			if statErr != nil || !input.IsSameFile(r.fs.Name(), info) {
				logp.Info("Force close file: %s; error: %s", r.fs.Name(), statErr)
				// Return directly on windows -> file is closing
				return n, errForceClose
			}
		}

		if err != io.EOF {
			logp.Err("Unexpected state reading from %s; error: %s", r.fs.Name(), err)
		}

		logp.Debug("harvester", "End of file reached: %s; Backoff now.", r.fs.Name())
		buf = buf[n:]
		if len(buf) == 0 {
			return n, nil
		}
		r.wait()
	}
}

func (r *logFileReader) wait() {
	// Wait before trying to read file wr.ch reached EOF again
	time.Sleep(r.backoff)

	// Increment backoff up to maxBackoff
	if r.backoff < r.config.maxBackoffDuration {
		r.backoff = r.backoff * time.Duration(r.config.backoffFactor)
		if r.backoff > r.config.maxBackoffDuration {
			r.backoff = r.config.maxBackoffDuration
		}
	}
}

func newBoundedLineReader(in lineReader, maxBytes int) *boundedLineReader {
	return &boundedLineReader{reader: in, maxBytes: maxBytes}
}

func (r *boundedLineReader) Next() (line, error) {
	line, err := r.reader.Next()
	if len(line.content) > r.maxBytes {
		line.content = line.content[:r.maxBytes]
	}
	return line, err
}

func newTimeoutLineReader(in lineReader, signal error, timeout time.Duration) *timeoutLineReader {
	if signal == nil {
		signal = errTimeout
	}

	return &timeoutLineReader{
		reader:  in,
		signal:  signal,
		timeout: timeout,
		ch:      make(chan lineMessage, 1),
	}
}

func (tr *timeoutLineReader) Next() (line, error) {
	if !tr.running {
		tr.running = true
		go func() {
			for {
				line, err := tr.reader.Next()
				tr.ch <- lineMessage{line, err}
				if err != nil {
					break
				}
			}
		}()
	}

	select {
	case msg := <-tr.ch:
		if msg.err != nil {
			tr.running = false
		}
		return msg.line, msg.err
	case <-time.After(tr.timeout):
		return line{}, tr.signal
	}
}
