package grpcevt

import (
	"time"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/rpcdef/datareq"
)

func decodeEvent(in *datareq.Event) beat.Event {
	ts := decodeTimestamp(in.Timestamp)

	return beat.Event{
		Timestamp: ts,
		Meta:      decodeObj(in.Meta),
		Fields:    decodeObj(in.Fields),
	}
}

func decodeTimestamp(in *datareq.Timestamp) time.Time {
	if in == nil {
		return time.Now()
	}

	ts := time.Unix(int64(in.Sec), int64(in.Nsec))
	off := int16(in.TimezoneOffset)

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

	return ts
}

func decodeObj(in *datareq.Object) common.MapStr {
	if in == nil || len(in.Fields) == 0 {
		return nil
	}

	m := make(common.MapStr, len(in.Fields))
	for k, v := range in.Fields {
		m[k] = decodeValue(v)
	}
	return m
}

func decodeArr(in *datareq.Array) []interface{} {
	if len(in.Values) == 0 {
		return nil
	}

	arr := make([]interface{}, len(in.Values))
	for i, v := range in.Values {
		arr[i] = decodeValue(v)
	}
	return arr
}

func decodeValue(in *datareq.Value) interface{} {
	switch v := in.Kind.(type) {
	case *datareq.Value_NullValue:
		return (common.MapStr)(nil)
	case *datareq.Value_StringValue:
		return v.StringValue
	case *datareq.Value_BoolValue:
		return v.BoolValue
	case *datareq.Value_IntValue:
		return v.IntValue
	case *datareq.Value_UintValue:
		return v.UintValue
	case *datareq.Value_DoubleValue:
		return v.DoubleValue
	case *datareq.Value_ObjectValue:
		return decodeObj(v.ObjectValue)
	case *datareq.Value_ArrayValue:
		return decodeArr(v.ArrayValue)
	case *datareq.Value_TimestampValue:
		return decodeTimestamp(v.TimestampValue)
	default:
		panic("Ooops")
	}
}
