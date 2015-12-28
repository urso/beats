package harvester

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/elastic/beats/filebeat/config"
)

type multiLineReader struct {
	reader   lineReader
	pred     matcher
	maxBytes int // bytes stored in content
	maxLines int

	content   []byte
	last      []byte
	readBytes int // bytes as read from input source
	numLines  int

	err   error // last seen error
	state func(*multiLineReader) ([]byte, int, error)
}

const (
	defaultMaxLines = 500
)

type matcher func(last, current []byte) bool

var (
	errMultilineTimeout = errors.New("multline timeout")
)

func newMultilineReader(
	r lineReader,
	maxBytes int,
	config *config.MultilineConfig,
) (*multiLineReader, error) {
	type matcherFactory func(pattern string) (matcher, error)
	types := map[string]matcherFactory{
		"before": beforeMatcher,
		"after":  afterMatcher,
	}

	matcherType, ok := types[config.Match]
	if !ok {
		return nil, fmt.Errorf("unknown matcher type: %s", config.Match)
	}

	matcher, err := matcherType(config.Pattern)
	if err != nil {
		return nil, err
	}

	if config.Negate {
		matcher = negatedMatcher(matcher)
	}

	maxLines := defaultMaxLines
	if config.MaxLines != nil {
		maxLines = *config.MaxLines
	}

	if config.Timeout != "" {
		timeout, err := time.ParseDuration(config.Timeout)
		if err != nil {
			return nil, fmt.Errorf("failed to parse duration '%s': %v", config.Timeout, err)
		}
		if timeout < 0 {
			return nil, fmt.Errorf("timeout %v must not be negative", config.Timeout)
		}
		r = newTimeoutLineReader(r, errMultilineTimeout, timeout)
	}

	mlr := &multiLineReader{
		reader:   r,
		pred:     matcher,
		state:    (*multiLineReader).readNext,
		maxBytes: maxBytes,
		maxLines: maxLines,
	}
	return mlr, nil
}

func (mlr *multiLineReader) Next() ([]byte, int, error) {
	return mlr.state(mlr)
}

func (mlr *multiLineReader) readNext() ([]byte, int, error) {
	for {
		line, sz, err := mlr.reader.Next()
		if err != nil {
			// handle multiline timeout signal
			if err == errMultilineTimeout {
				// no lines buffered -> ignore timeout
				if mlr.numLines == 0 {
					continue
				}

				// return collected multiline event and empty buffer for new multiline event
				line, sz := mlr.pushLine()
				return line, sz, nil
			}

			// handle error without any bytes returned from reader
			if sz == 0 {
				// no lines buffered -> return error
				if mlr.numLines == 0 {
					return nil, 0, err
				}

				// lines buffered, return multiline and error on next read
				line, sz := mlr.pushLine()
				mlr.err = err
				mlr.state = (*multiLineReader).readFailed
				return line, sz, nil
			}

			// handle error with some content being returned by reader and
			// line matching multiline criteria or no multiline started yet
			if mlr.readBytes == 0 || mlr.pred(mlr.last, line) {
				mlr.addLine(line, sz)

				// return multiline and error on next read
				line, sz := mlr.pushLine()
				mlr.err = err
				mlr.state = (*multiLineReader).readFailed
				return line, sz, nil
			}

			// no match, return current multline and retry with current line on next
			// call to readNext awaiting the error being reproduced (or resolved)
			// in next call to Next
			line, sz := mlr.startNewLine(line, sz)
			return line, sz, nil
		}

		// if predicate does not match current multiline -> return multiline event
		if mlr.readBytes > 0 && !mlr.pred(mlr.last, line) {
			line, sz := mlr.startNewLine(line, sz)
			return line, sz, nil
		}

		// add line to current multiline event
		mlr.addLine(line, sz)
	}
}

func (mlr *multiLineReader) readFailed() ([]byte, int, error) {
	// return error and reset line reader
	err := mlr.err
	mlr.err = nil
	mlr.state = (*multiLineReader).readNext
	return nil, 0, err
}

func (mlr *multiLineReader) startNewLine(line []byte, sz int) ([]byte, int) {
	retLine, retSize := mlr.pushLine()
	mlr.addLine(line, sz)
	return retLine, retSize
}

func (mlr *multiLineReader) pushLine() ([]byte, int) {
	line := mlr.content
	sz := mlr.readBytes

	mlr.content = nil
	mlr.last = nil
	mlr.readBytes = 0
	mlr.numLines = 0
	mlr.err = nil

	return line, sz
}

func (mlr *multiLineReader) addLine(line []byte, sz int) {
	if sz <= 0 {
		return
	}

	space := mlr.maxBytes - len(mlr.content)
	if (mlr.maxBytes <= 0 || space > 0) && (mlr.maxLines <= 0 || mlr.numLines < mlr.maxLines) {
		if space < 0 || space > len(line) {
			space = len(line)
		}
		mlr.content = append(mlr.content, line[:space]...)
		mlr.numLines++
	}

	mlr.last = line
	mlr.readBytes += sz
}

// matchers

func afterMatcher(pattern string) (matcher, error) {
	return genPatternMatcher(pattern, func(last, current []byte) []byte {
		return current
	})
}

func beforeMatcher(pattern string) (matcher, error) {
	return genPatternMatcher(pattern, func(last, current []byte) []byte {
		return last
	})
}

func negatedMatcher(m matcher) matcher {
	return func(last, current []byte) bool {
		return !m(last, current)
	}
}

func genPatternMatcher(pattern string, sel func(last, current []byte) []byte) (matcher, error) {
	reg, err := regexp.CompilePOSIX(pattern)
	if err != nil {
		return nil, err
	}

	matcher := func(last, current []byte) bool {
		line := sel(last, current)
		return reg.Match(line)
	}
	return matcher, nil
}
