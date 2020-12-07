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
	"bytes"
	"fmt"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/outputs/codec"
	"github.com/elastic/go-structform/cborl"
	"github.com/elastic/go-structform/gotype"
)

// Encoder for serializing a beat.Event to json.
type Encoder struct {
	buf    bytes.Buffer
	folder *gotype.Iterator

	version string
}

// New creates a new json Encoder.
func New(version string) *Encoder {
	e := &Encoder{version: version}
	e.reset()
	return e
}

func (e *Encoder) reset() {
	visitor := cborl.NewVisitor(&e.buf)

	var err error

	// create new encoder with custom time.Time encoding
	e.folder, err = gotype.NewIterator(visitor,
		gotype.Folders(
			codec.MakeUTCOrLocalTimestampEncoder(false),
			codec.MakeBCTimestampEncoder(),
		),
	)
	if err != nil {
		panic(err)
	}
}

// Encode serializes a beat event to JSON. It adds additional metadata in the
// `@metadata` namespace.
func (e *Encoder) Encode(index string, event *beat.Event) ([]byte, error) {
	e.buf.Reset()
	err := e.folder.Fold(makeEvent(index, e.version, event))
	if err != nil {
		e.reset()
		return nil, err
	}

	bytes := e.buf.Bytes()

	// tryDecode(bytes, event)

	return bytes, nil
}

func tryDecode(buf []byte, origin *beat.Event) {
	var tmp event
	unfolder, _ := gotype.NewUnfolder(&tmp)
	err := cborl.Parse(buf, unfolder)
	if err != nil {
		fmt.Printf("Failed to parse event: %v\nbuffer: %v", origin, buf)
		panic(err)
	}

	fmt.Println("New CBOR event: ", buf)
}
