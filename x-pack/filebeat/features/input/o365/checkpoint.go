package o365

import (
	"time"

	cursor "github.com/elastic/beats/v7/filebeat/input/v2/input-cursor"
	"github.com/elastic/beats/v7/libbeat/logp"
)

// A cursor represents a point in time within an event stream
// that can be persisted and used to resume processing from that point.
type checkpoint struct {
	// createdTime for the last seen blob.
	Timestamp time.Time
	// index of object count (1...n) within a blob.
	Line int
	// startTime used in the last list content query.
	// This is necessary to ensure that the same blobs are observed.
	StartTime time.Time
}

func makeCheckpoint(log *logp.Logger, c cursor.Cursor, maxRetention time.Duration) checkpoint {
	var cp checkpoint
	retentionLimit := time.Now().UTC().Add(-maxRetention)

	if c.IsNew() {
		cp.Timestamp = retentionLimit
	} else {
		err := c.Unpack(&cp)
		if err != nil {
			log.Errorw("Error loading saved state. Will fetch all retained events. "+
				"Depending on max_retention, this can cause event loss or duplication.",
				"error", err,
				"max_retention", maxRetention.String())
			cp.Timestamp = retentionLimit
		}
	}

	if cp.Timestamp.Before(retentionLimit) {
		log.Warnw("Last update exceeds the retention limit. "+
			"Probably some events have been lost.",
			"resume_since", cp,
			"retention_limit", retentionLimit,
			"max_retention", maxRetention.String())
		// Due to API limitations, it's necessary to perform a query for each
		// day. These avoids performing a lot of queries that will return empty
		// when the input hasn't run in a long time.
		cp.Timestamp = retentionLimit
	}

	return cp
}

// WithStartTime allows to create a checkpoint with an updated startTime.
func (cp checkpoint) WithStartTime(ts time.Time) checkpoint {
	cp.StartTime = ts
	return cp
}

func (cp checkpoint) ForNextLine() checkpoint {
	cp.Line++
	return cp
}

// TryAdvance advances the cursor to the given content blob
// if it's not in the past.
// Returns whether the given content needs to be processed.
func (c *checkpoint) TryAdvance(ct content) bool {
	if ct.Created.Before(c.Timestamp) {
		return false
	}
	if ct.Created.Equal(c.Timestamp) {
		// Only need to re-process the current content blob if we're
		// seeking to a line inside it.
		return c.Line > 0
	}
	c.Timestamp = ct.Created
	c.Line = 0
	return true
}
