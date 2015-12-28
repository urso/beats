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

	ts        time.Time
	content   []byte
	last      []byte
	readBytes int // bytes as read from input source
	numLines  int

	err   error // last seen error
	state func(*multiLineReader) (line, error)
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

func (mlr *multiLineReader) Next() (line, error) {
	return mlr.state(mlr)
}

func (mlr *multiLineReader) readNext() (line, error) {
	for {
		l, err := mlr.reader.Next()
		if err != nil {
			// handle multiline timeout signal
			if err == errMultilineTimeout {
				// no lines buffered -> ignore timeout
				if mlr.numLines == 0 {
					continue
				}

				// return collected multiline event and empty buffer for new multiline event
				l := mlr.pushLine()
				return l, nil
			}

			// handle error without any bytes returned from reader
			if l.sz == 0 {
				// no lines buffered -> return error
				if mlr.numLines == 0 {
					return line{}, err
				}

				// lines buffered, return multiline and error on next read
				l := mlr.pushLine()
				mlr.err = err
				mlr.state = (*multiLineReader).readFailed
				return l, nil
			}

			// handle error with some content being returned by reader and
			// line matching multiline criteria or no multiline started yet
			if mlr.readBytes == 0 || mlr.pred(mlr.last, l.content) {
				mlr.addLine(l)

				// return multiline and error on next read
				l := mlr.pushLine()
				mlr.err = err
				mlr.state = (*multiLineReader).readFailed
				return l, nil
			}

			// no match, return current multline and retry with current line on next
			// call to readNext awaiting the error being reproduced (or resolved)
			// in next call to Next
			l := mlr.startNewLine(l)
			return l, nil
		}

		// if predicate does not match current multiline -> return multiline event
		if mlr.readBytes > 0 && !mlr.pred(mlr.last, l.content) {
			l := mlr.startNewLine(l)
			return l, nil
		}

		// add line to current multiline event
		mlr.addLine(l)
	}
}

func (mlr *multiLineReader) readFailed() (line, error) {
	// return error and reset line reader
	err := mlr.err
	mlr.err = nil
	mlr.state = (*multiLineReader).readNext
	return line{}, err
}

func (mlr *multiLineReader) startNewLine(l line) line {
	retLine := mlr.pushLine()
	mlr.addLine(l)
	mlr.ts = l.ts
	return retLine
}

func (mlr *multiLineReader) pushLine() line {
	content := mlr.content
	sz := mlr.readBytes

	mlr.content = nil
	mlr.last = nil
	mlr.readBytes = 0
	mlr.numLines = 0
	mlr.err = nil

	return line{ts: mlr.ts, content: content, sz: sz}
}

func (mlr *multiLineReader) addLine(l line) {
	if l.sz <= 0 {
		return
	}

	space := mlr.maxBytes - len(mlr.content)
	if (mlr.maxBytes <= 0 || space > 0) && (mlr.maxLines <= 0 || mlr.numLines < mlr.maxLines) {
		if space < 0 || space > len(l.content) {
			space = len(l.content)
		}
		mlr.content = append(mlr.content, l.content[:space]...)
		mlr.numLines++
	}

	mlr.last = l.content
	mlr.readBytes += l.sz
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
