package journalread

import (
	"errors"
	"fmt"
)

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

// Unpack validates and unpack "seek" config option
func (m *SeekMode) Unpack(value string) error {
	mode, ok := seekModes[value]
	if !ok {
		return fmt.Errorf("invalid seek mode '%s'", value)
	}

	*m = mode
	return nil
}
