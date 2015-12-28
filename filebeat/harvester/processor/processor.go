package processor

import (
	"io"
	"time"

	"github.com/elastic/beats/filebeat/harvester/encoding"
)

type Line struct {
	Ts      time.Time
	Content []byte
	Bytes   int
}

type LineProcessor interface {
	Next() (Line, error)
}

type LineSource struct {
	reader *encoding.LineReader
}

// noEOLLineReader strips last EOL from input line reader (if present).
type StripNewline struct {
	reader LineProcessor
}

type LimitProcessor struct {
	reader   LineProcessor
	maxBytes int
}

type lineMessage struct {
	line Line
	err  error
}

func NewLineSource(
	in io.Reader,
	codec encoding.Encoding,
	bufferSize int,
) (LineSource, error) {
	r, err := encoding.NewLineReader(in, codec, bufferSize)
	return LineSource{r}, err
}

func (r LineSource) Next() (Line, error) {
	c, sz, err := r.reader.Next()
	return Line{Ts: time.Now(), Content: c, Bytes: sz}, err
}

func NewStripNewline(r LineProcessor) *StripNewline {
	return &StripNewline{r}
}

func (n *StripNewline) Next() (Line, error) {
	line, err := n.reader.Next()
	if err != nil {
		return line, err
	}

	L := line.Content
	line.Content = L[:len(L)-lineEndingChars(L)]
	return line, err
}

func NewLimitProcessor(in LineProcessor, maxBytes int) *LimitProcessor {
	return &LimitProcessor{reader: in, maxBytes: maxBytes}
}

func (r *LimitProcessor) Next() (Line, error) {
	line, err := r.reader.Next()
	if len(line.Content) > r.maxBytes {
		line.Content = line.Content[:r.maxBytes]
	}
	return line, err
}

// isLine checks if the given byte array is a line, means has a line ending \n
func isLine(l []byte) bool {
	return l != nil && len(l) > 0 && l[len(l)-1] == '\n'
}

// lineEndingChars returns the number of line ending chars the given by array has
// In case of Unix/Linux files, it is -1, in case of Windows mostly -2
func lineEndingChars(l []byte) int {
	if !isLine(l) {
		return 0
	}

	if len(l) > 1 && l[len(l)-2] == '\r' {
		return 2
	}
	return 1
}
