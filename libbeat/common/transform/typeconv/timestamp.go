package typeconv

import (
	"errors"
	"fmt"
	"time"

	structform "github.com/elastic/go-structform"
	"github.com/elastic/go-structform/gotype"
)

type timeUnfolder struct {
	gotype.BaseUnfoldState
	to   *time.Time
	a, b uint64
	st   timeUnfoldState
}

type timeUnfoldState uint8

const (
	timeUnfoldInit timeUnfoldState = iota
	timeUnfoldDone
	timeUnfoldWaitA
	timeUnfoldWaitB
	timeUnfoldWaitDone
)

func FoldTimestampToArray(in *time.Time, v structform.ExtVisitor) error {
	extra, sec := timestampToBits(*in)

	if err := v.OnArrayStart(2, structform.Uint64Type); err != nil {
		return err
	}
	if err := v.OnUint64(extra); err != nil {
		return err
	}
	if err := v.OnUint64(sec); err != nil {
		return err
	}
	if err := v.OnArrayFinished(); err != nil {
		return err
	}

	return nil
}

func TimestampArrayUnfolder(to *time.Time) gotype.UnfoldState {
	return &timeUnfolder{to: to}
}

func (u *timeUnfolder) OnString(ctx gotype.UnfoldCtx, in string) (err error) {
	if u.st != timeUnfoldInit {
		return fmt.Errorf("Unexpected string '%v' when trying to unfold a timestamp", in)
	}

	*u.to, err = time.Parse(time.RFC3339, in)
	u.st = timeUnfoldDone
	ctx.Done()
	return err
}

func (u *timeUnfolder) OnArrayStart(ctx gotype.UnfoldCtx, len int, _ structform.BaseType) error {
	if u.st != timeUnfoldInit {
		return errors.New("unexpected array")
	}

	if len >= 0 && len != 2 {
		return fmt.Errorf("%v is no valid encoded timestamp length", len)
	}

	u.st = timeUnfoldWaitA
	return nil
}

func (u *timeUnfolder) OnInt(ctx gotype.UnfoldCtx, in int64) error {
	return u.OnUint(ctx, uint64(in))
}
func (u *timeUnfolder) OnFloat(ctx gotype.UnfoldCtx, f float64) error {
	return u.OnUint(ctx, uint64(f))
}
func (u *timeUnfolder) OnUint(ctx gotype.UnfoldCtx, in uint64) error {
	switch u.st {
	case timeUnfoldWaitA:
		u.a = in
		u.st = timeUnfoldWaitB
	case timeUnfoldWaitB:
		u.b = in
		u.st = timeUnfoldWaitDone
	default:
		return fmt.Errorf("unexpected number '%v' in timestamp array", in)
	}
	return nil
}

func (u *timeUnfolder) OnArrayFinished(ctx gotype.UnfoldCtx) error {
	defer ctx.Done()

	if u.st != timeUnfoldWaitDone {
		return errors.New("unexpected timestamp array closed")
	}

	u.st = timeUnfoldDone

	ts, err := bitsToTimestamp(u.a, u.b)
	if err != nil {
		return err
	}
	*u.to = ts

	return nil
}

func timestampToBits(ts time.Time) (uint64, uint64) {
	var (
		off int16
		loc = ts.Location()
	)

	const encodingVersion = 0

	if loc == time.UTC {
		off = -1
	} else {
		_, offset := ts.Zone()
		offset /= 60 // Note: best effort. If the zone offset has a factional minute, then we will ignore it here
		if offset < -32768 || offset == -1 || offset > 32767 {
			offset = 0 // Note: best effort. Ignore offset if it becomes an unexpected value
		}
		off = int16(offset)
	}

	sec := uint64(ts.Unix())
	extra := (uint64(encodingVersion) << 56) |
		(uint64(uint16(off)) << 32) |
		uint64(ts.Nanosecond())

	return extra, sec
}

func bitsToTimestamp(extra, sec uint64) (time.Time, error) {
	var ts time.Time

	version := (extra >> 56) & 0xff
	if version != 0 {
		return ts, fmt.Errorf("invalid timestamp [%x, %x]", extra, sec)
	}

	nsec := uint32(extra)
	off := int16(extra >> 32)
	ts = time.Unix(int64(sec), int64(nsec))

	// adjust location by offset. time.Unix creates a timestamp in the local zone
	// by default. Only change this if off does not match the local zone it's offset.
	if off == -1 {
		ts = ts.UTC()
	} else if off != 0 {
		_, locOff := ts.Zone()
		if off != int16(locOff/60) {
			ts = ts.In(time.FixedZone("", int(off*60)))
		}
	}

	return ts, nil
}
