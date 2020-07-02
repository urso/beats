package journalread

import (
	"errors"
	"fmt"
)

// SeekMode is used by (*Reader).Seek to decide where to advance the read pointer to.
type SeekMode uint

const (
	// SeekInvalid is an invalid value for seek
	SeekInvalid SeekMode = iota
	// SeekHead option seeks to the head of a journal
	SeekHead
	// SeekTail option seeks to the tail of a journal
	SeekTail
	// SeekCursor option seeks to the position specified in the cursor
	SeekCursor
)

var seekModes = map[string]SeekMode{
	"head":   SeekHead,
	"tail":   SeekTail,
	"cursor": SeekCursor,
}

var errInvalidSeekFallback = errors.New("invalid setting for cursor_seek_fallback")

// Unpack validates and unpack "seek" config options. It returns an error if
// the string is no valid seek mode.
func (m *SeekMode) Unpack(value string) error {
	mode, ok := seekModes[value]
	if !ok {
		return fmt.Errorf("invalid seek mode '%s'", value)
	}

	*m = mode
	return nil
}
