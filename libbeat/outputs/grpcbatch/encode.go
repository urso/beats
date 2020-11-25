package grpcbatch

import (
	"time"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/rpcdef/datareq"
)

var (
	nullValue  = &datareq.Value{Kind: &datareq.Value_NullValue{}}
	trueValue  = &datareq.Value{Kind: &datareq.Value_BoolValue{BoolValue: true}}
	falseValue = &datareq.Value{Kind: &datareq.Value_BoolValue{BoolValue: false}}
)

func encodeEvent(event *beat.Event) (*datareq.Event, error) {
	rpcEvent := &datareq.Event{
		Timestamp: encTimestamp(event.Timestamp),
		Meta:      encMapStr(event.Meta),
		Fields:    encMapStr(event.Fields),
	}
	return rpcEvent, nil
}

func encTimestamp(ts time.Time) *datareq.Timestamp {
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

	return &datareq.Timestamp{
		Sec:            uint64(ts.Unix()),
		Nsec:           uint32(ts.Nanosecond()),
		TimezoneOffset: uint32(off),
	}
}

func encMapStr(ms common.MapStr) *datareq.Object {
	if len(ms) == 0 {
		return nil
	}

	fields := make(map[string]*datareq.Value, len(ms))
	for k, v := range ms {
		fields[k] = encAny(v)
	}

	return &datareq.Object{Fields: fields}
}

func encAny(in interface{}) *datareq.Value {
	if in == nil {
		return nullValue
	}

	switch v := in.(type) {
	// primitive types
	case bool:
		if v {
			return trueValue
		}
		return falseValue
	case string:
		return newStringValue(v)
	case int8:
		return newIntValue(int64(v))
	case int16:
		return newIntValue(int64(v))
	case int32:
		return newIntValue(int64(v))
	case int64:
		return newIntValue(v)
	case int:
		return newIntValue(int64(v))
	case uint8:
		return newUintValue(uint64(v))
	case uint16:
		return newUintValue(uint64(v))
	case uint32:
		return newUintValue(uint64(v))
	case uint64:
		return newUintValue(v)
	case uint:
		return newUintValue(uint64(v))
	case float32:
		return newDoubleValue(float64(v))
	case float64:
		return newDoubleValue(v)
	case common.Float:
		return newDoubleValue(float64(v))

		// custom primitives
	case time.Time:
		return newTimestamp(v)
	case common.Time:
		return newTimestamp(time.Time(v))

		// array variants:
	case []string:
		arr := make([]*datareq.Value, len(v))
		for i, str := range v {
			arr[i] = newStringValue(str)
		}
		return newArray(arr)

	case []uint64:
		panic("TODO []uint64")

		// object variants
	case common.MapStr:
		obj := encMapStr(v)
		if obj == nil {
			return nullValue
		}
		return &datareq.Value{Kind: &datareq.Value_ObjectValue{ObjectValue: obj}}

	default:
		// XXX: this approach does not necessarily scale well...
		panic("unsupported type")

	}
}

func newArray(arr []*datareq.Value) *datareq.Value {
	if len(arr) == 0 {
		return nullValue
	}
	return &datareq.Value{Kind: &datareq.Value_ArrayValue{ArrayValue: &datareq.Array{Values: arr}}}
}

func newStringValue(str string) *datareq.Value {
	return &datareq.Value{Kind: &datareq.Value_StringValue{StringValue: str}}
}

func newIntValue(i int64) *datareq.Value {
	return &datareq.Value{Kind: &datareq.Value_IntValue{IntValue: i}}
}

func newUintValue(u uint64) *datareq.Value {
	return &datareq.Value{Kind: &datareq.Value_UintValue{UintValue: u}}
}

func newDoubleValue(f float64) *datareq.Value {
	return &datareq.Value{Kind: &datareq.Value_DoubleValue{DoubleValue: f}}
}

func newTimestamp(ts time.Time) *datareq.Value {
	return &datareq.Value{Kind: &datareq.Value_TimestampValue{TimestampValue: encTimestamp(ts)}}
}
