// +build linux,cgo

package journald

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/coreos/go-systemd/v22/sdjournal"
)

// Config stores the options of a journald input.
type config struct {
	// Paths stores the paths to the journal files to be read.
	Paths []string `config:"paths"`

	// Backoff is the current interval to wait before
	// attemting to read again from the journal.
	Backoff time.Duration `config:"backoff" validate:"min=0,nonzero"`

	// MaxBackoff is the limit of the backoff time.
	MaxBackoff time.Duration `config:"max_backoff" validate:"min=0,nonzero"`

	// Seek is the method to read from journals.
	Seek seekMode `config:"seek"`

	// CursorSeekFallback sets where to seek if registry file is not available.
	CursorSeekFallback seekMode `config:"cursor_seek_fallback"`

	// Matches store the key value pairs to match entries.
	Matches []matcher `config:"include_matches"`

	// SaveRemoteHostname defines if the original source of the entry needs to be saved.
	SaveRemoteHostname bool `config:"save_remote_hostname"`
}

type seekMode uint

type matcher string

const (
	// seekInvalid is an invalid value for seek
	seekInvalid seekMode = iota
	// seekHead option seeks to the head of a journal
	seekHead
	// seekTail option seeks to the tail of a journal
	seekTail
	// SeekCursor option seeks to the position specified in the cursor
	seekCursor
)

var seekModes = map[string]seekMode{
	"head":   seekHead,
	"tail":   seekTail,
	"cursor": seekCursor,
}

var errInvalidSeekFallback = errors.New("invalid setting for cursor_seek_fallback")

func defaultConfig() config {
	return config{
		Backoff:            1 * time.Second,
		MaxBackoff:         20 * time.Second,
		Seek:               seekCursor,
		CursorSeekFallback: seekHead,
		SaveRemoteHostname: false,
	}
}

func (c *config) Validate() error {
	if c.CursorSeekFallback != seekHead && c.CursorSeekFallback != seekTail {
		return errInvalidSeekFallback
	}
	return nil
}

// Unpack validates and unpack "seek" config option
func (m *seekMode) Unpack(value string) error {
	mode, ok := seekModes[value]
	if !ok {
		return fmt.Errorf("invalid seek mode '%s'", value)
	}

	*m = mode
	return nil
}

func (m *matcher) Unpack(value string) error {
	tmp, err := compileMatch(value)
	if err != nil {
		return err
	}
	*m = tmp
	return nil
}

func (m matcher) Apply(j *sdjournal.Journal) error {
	err := j.AddMatch(string(m))
	if err != nil {
		return fmt.Errorf("error adding match '%s' to journal: %v", m, err)
	}
	return nil
}

func compileMatch(in string) (matcher, error) {
	elems := strings.Split(in, "=")
	if len(elems) != 2 {
		return "", fmt.Errorf("invalid match format: %s", in)
	}

	for journalKey, eventField := range journaldEventFields {
		if elems[0] == eventField.name {
			return matcher(journalKey + "=" + elems[1]), nil
		}
	}

	// pass custom fields as is
	return matcher(in), nil
}

func applyMatchers(j *sdjournal.Journal, matchers []matcher) error {
	for _, m := range matchers {
		if err := m.Apply(j); err != nil {
			return err
		}

		if err := j.AddDisjunction(); err != nil {
			return fmt.Errorf("error adding disjunction to journal: %v", err)
		}
	}

	return nil
}
