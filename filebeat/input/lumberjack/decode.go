package lumberjack

import (
	"encoding/json"
	"reflect"
	"time"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
)

type jsonDecoder struct {
	logger *logp.Logger
}

func (j *jsonDecoder) Decode(in []byte, v interface{}) error {
	var raw map[string]interface{}
	err := json.Unmarshal(in, &raw)
	if err != nil {
		return err
	}

	var eventMeta map[string]interface{}
	if metaRaw, ok := raw["@metadata"]; ok {
		delete(raw, "@metadata")
		if metaMap, ok := metaRaw.(map[string]interface{}); ok {
			eventMeta = metaMap
		}
	}

	timestamp := extractTimestamp(raw)

	event := beat.Event{
		Timestamp: timestamp,
		Meta:      common.MapStr(eventMeta),
		Fields:    common.MapStr(raw),
	}

	rv := reflect.ValueOf(v)
	rv.Elem().Set(reflect.ValueOf(event))
	return nil
}

func extractTimestamp(raw map[string]interface{}) time.Time {
	if tsRaw, ok := raw["@timestamp"]; ok {
		delete(raw, "@timestamp")
		if tsString, ok := tsRaw.(string); ok {
			if parsed, err := common.ParseTime(tsString); err == nil {
				return time.Time(parsed)
			}
		}
	}

	return time.Now()
}
