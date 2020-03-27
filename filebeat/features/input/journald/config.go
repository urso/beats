package journald

import (
	"fmt"
	"time"
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
	Matches []string `config:"include_matches"`

	// SaveRemoteHostname defines if the original source of the entry needs to be saved.
	SaveRemoteHostname bool `config:"save_remote_hostname"`
}

type seekMode uint

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

func defaultConfig() config {
	return config{
		Backoff:            1 * time.Second,
		MaxBackoff:         20 * time.Second,
		Seek:               seekCursor,
		CursorSeekFallback: seekHead,
		SaveRemoteHostname: false,
	}
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
