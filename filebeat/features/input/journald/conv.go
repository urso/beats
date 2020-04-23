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

package journald

import (
	"strconv"
	"strings"
	"time"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
)

type eventConverter struct {
	log                *logp.Logger
	saveRemoteHostname bool
}

type fieldConversion struct {
	name      string
	isInteger bool
	dropped   bool
}

func (conv *eventConverter) Convert(
	timestamp uint64,
	entryFields map[string]string,
	convFields map[string]fieldConversion,
) beat.Event {
	created := time.Now()
	fields := common.MapStr{}
	var custom common.MapStr

	for entryKey, v := range entryFields {
		if fieldConversionInfo, ok := convFields[entryKey]; !ok {
			if custom == nil {
				custom = common.MapStr{}
			}
			normalized := strings.ToLower(strings.TrimLeft(entryKey, "_"))
			custom.Put(normalized, v)
		} else if !fieldConversionInfo.dropped {
			value := conv.convertNamedField(fieldConversionInfo, v)
			fields.Put(fieldConversionInfo.name, value)
		}
	}

	if len(custom) != 0 {
		fields.Put("journald.custom", custom)
	}

	// if entry is coming from a remote journal, add_host_metadata overwrites the source hostname, so it
	// has to be copied to a different field
	if conv.saveRemoteHostname {
		remoteHostname, err := fields.GetValue("host.hostname")
		if err == nil {
			fields.Put("log.source.address", remoteHostname)
		}
	}

	fields.Put("event.created", created)
	receivedByJournal := time.Unix(0, int64(timestamp)*1000)

	return beat.Event{
		Timestamp: receivedByJournal,
		Fields:    fields,
	}
}

func (conv *eventConverter) convertNamedField(fc fieldConversion, value string) interface{} {
	if fc.isInteger {
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			// On some versions of systemd the 'syslog.pid' can contain the username
			// appended to the end of the pid. In most cases this does not occur
			// but in the cases that it does, this tries to strip ',\w*' from the
			// value and then perform the conversion.
			s := strings.Split(value, ",")
			v, err = strconv.ParseInt(s[0], 10, 64)
			if err != nil {
				conv.log.Debugf("Failed to convert field: %s \"%v\" to int: %v", fc.name, value, err)
				return value
			}
		}
		return v
	}
	return value
}
