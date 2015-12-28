package processor

import (
	"errors"
	"io"
	"time"

	"github.com/elastic/beats/filebeat/harvester/encoding"
)

type line struct {
	Ts      time.Time
	Content []byte
	Bytes   int
}

type LineProcessor interface {
	Next() (line, error)
}

type encLineReader struct {
	reader *encoding.LineReader
}

// noEOLLineReader strips last EOL from input line reader (if present).
type stripNLLineProcessor struct {
	reader LineProcessor
}

type boundedLineReader struct {
	reader   LineProcessor
	maxBytes int
}

type timeoutLineReader struct {
	reader  LineProcessor
	timeout time.Duration
	signal  error

	running bool
	ch      chan lineMessage
}

type lineMessage struct {
	line line
	err  error
}

var (
	errTimeout = errors.New("timeout")
)

func NewEncLineReader(
	in io.Reader,
	codec encoding.Encoding,
	bufferSize int,
) (encLineReader, error) {
	r, err := encoding.NewLineReader(in, codec, bufferSize)
	return encLineReader{r}, err
}

func (r encLineReader) Next() (line, error) {
	c, sz, err := r.reader.Next()
	return line{Ts: time.Now(), Content: c, Bytes: sz}, err
}

func NewStripNLLineProcessor(r LineProcessor) *stripNLLineProcessor {
	return &stripNLLineProcessor{r}
}

func (n *stripNLLineProcessor) Next() (line, error) {
	line, err := n.reader.Next()
	if err != nil {
		return line, err
	}

	L := line.Content
	line.Content = L[:len(L)-lineEndingChars(L)]
	return line, err
}

func NewBoundedLineReader(in LineProcessor, maxBytes int) *boundedLineReader {
	return &boundedLineReader{reader: in, maxBytes: maxBytes}
}

func (r *boundedLineReader) Next() (line, error) {
	line, err := r.reader.Next()
	if len(line.Content) > r.maxBytes {
		line.Content = line.Content[:r.maxBytes]
	}
	return line, err
}

func newTimeoutLineReader(in LineProcessor, signal error, timeout time.Duration) *timeoutLineReader {
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

// isLine checks if the given byte array is a line, means has a line ending \n
func isLine(line []byte) bool {
	if line == nil || len(line) == 0 {
		return false
	}

	if line[len(line)-1] != '\n' {
		return false
	}
	return true
}

// lineEndingChars returns the number of line ending chars the given by array has
// In case of Unix/Linux files, it is -1, in case of Windows mostly -2
func lineEndingChars(line []byte) int {
	if !isLine(line) {
		return 0
	}

	if line[len(line)-1] == '\n' {
		if len(line) > 1 && line[len(line)-2] == '\r' {
			return 2
		}

		return 1
	}
	return 0
}
