// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package cbor

import (
	"time"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
)

// Event describes the event structure for events
// (in-)directly send to logstash
type event struct {
	Timestamp_Sec  uint64        `struct:"ts_sec"`
	Timestamp_NSec uint32        `struct:"ts_nsec"`
	Timestamp_Off  uint16        `struct:"ts_off"`
	Meta           common.MapStr `struct:"metadata"`
	Fields         common.MapStr `struct:"fields"`
}

func makeEvent(index, version string, in *beat.Event) event {
	ts := in.Timestamp

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

	return event{
		Timestamp_Sec:  uint64(ts.Unix()),
		Timestamp_NSec: uint32(ts.Nanosecond()),
		Timestamp_Off:  uint16(off),
		Meta:           in.Meta,
		Fields:         in.Fields,
	}
}
