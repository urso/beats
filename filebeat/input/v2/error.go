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

package v2

import (
	"errors"
	"strings"
)

type Error struct {
	op      string
	name    string
	kind    error
	message string
	cause   error
}

var (
	ErrInitLoader   = errors.New("can not create loader")
	ErrUnknownInput = errors.New("unknown input")
	ErrCreateInput  = errors.New("failed to initialize the input")
)

func (e *Error) Op() string {
	return e.op
}

func (e *Error) Kind() error {
	return e.kind
}

func (e *Error) Unwrap() error {
	return e.cause
}

func (e *Error) Error() string {
	var buf strings.Builder

	pad := func(sep string) {
		if buf.Len() != 0 {
			buf.WriteString(sep)
		}
	}

	if e.op != "" {
		buf.WriteString(e.op)
	}
	if e.kind != nil {
		pad(": ")
		buf.WriteString(e.kind.Error())
	}

	if e.name != "" {
		pad(": ")
		buf.WriteString(e.name)
	}

	if e.message != "" {
		pad(": ")
		buf.WriteString(e.message)
	}

	if e.cause != nil {
		cause := e.cause.Error()
		if cause != "" {
			pad(": ")
			buf.WriteString(cause)
		}
	}

	return buf.String()
}
