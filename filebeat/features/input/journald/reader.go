package journald

import (
	"fmt"
	"io"
	"syscall"
	"time"

	"github.com/coreos/go-systemd/v22/sdjournal"
	input "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/common/backoff"
	"github.com/elastic/beats/v7/libbeat/logp"
)

type reader struct {
	log     *logp.Logger
	journal *sdjournal.Journal
	backoff backoff.Backoff
}

func (r *reader) Seek(mode seekMode, cursor string) (err error) {
	switch mode {
	case seekHead:
		err = r.journal.SeekHead()
	case seekTail:
		if err = r.journal.SeekTail(); err == nil {
			_, err = r.journal.Next()
		}
	case seekCursor:
		if err = r.journal.SeekCursor(cursor); err == nil {
			_, err = r.journal.Next()
		}
	default:
		return fmt.Errorf("invalid seek mode '%v'", mode)
	}
	return err
}

func (r *reader) Next(cancel input.Canceler) (*sdjournal.JournalEntry, error) {
	for cancel.Err() == nil {
		c, err := r.journal.Next()
		if err != nil && err != io.EOF {
			return nil, err
		}

		switch {
		// error while reading next entry
		case c < 0:
			return nil, fmt.Errorf("error while reading next entry %+v", syscall.Errno(-c))
		// no new entry, so wait
		case c == 0:
			hasNewEntry, err := r.checkForNewEvents()
			if err != nil {
				return nil, err
			}
			if !hasNewEntry {
				// TODO: backoff support is currently not cancellable :(
				r.backoff.Wait()
			}
			continue
		// new entries are available
		default:
		}

		entry, err := r.journal.GetEntry()
		if err != nil {
			return nil, err
		}
		r.backoff.Reset()

		return entry, nil
	}
	return nil, cancel.Err()
}

func (r *reader) checkForNewEvents() (bool, error) {
	c := r.journal.Wait(100 * time.Millisecond)
	switch c {
	case sdjournal.SD_JOURNAL_NOP:
		return false, nil
	// new entries are added or the journal has changed (e.g. vacuum, rotate)
	case sdjournal.SD_JOURNAL_APPEND, sdjournal.SD_JOURNAL_INVALIDATE:
		return true, nil
	default:
	}

	r.log.Errorf("Unknown return code from Wait: %d\n", c)
	return false, nil
}
