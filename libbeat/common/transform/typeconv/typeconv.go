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

package typeconv

import (
	"sync"

	"github.com/elastic/go-structform/gotype"
)

// Converter converts structured data between arbitrary typed (serializable)
// go structures and maps/slices/arrays. It uses go-structform/gotype for input
// and output values each, such that any arbitrary structures can be used.
//
// The converter computes and caches mapping operations for go structures it
// has visited.
type Converter struct {
	fold   *gotype.Iterator
	unfold *gotype.Unfolder
}

var convPool = sync.Pool{
	New: func() interface{} {
		return &Converter{}
	},
}

// NewConverter creates a new converter with local state for tracking known
// type conversations.
func NewConverter() *Converter {
	c := &Converter{}
	return c
}

func (c *Converter) init() {
	unfold, _ := gotype.NewUnfolder(nil, gotype.Unfolders(
		TimestampArrayUnfolder,
	))
	fold, err := gotype.NewIterator(unfold, gotype.Folders(
		FoldTimestampToArray,
	))
	if err != nil {
		panic(err)
	}

	c.unfold = unfold
	c.fold = fold
}

// Convert transforms the value of from into to, by translating the structure
// from into a set of events (go-structform.Visitor) that can applied to the
// value given by to.
// The operation fails if the values are not compatible (for example trying to
// convert an object into an int), or `to` is no pointer.
func (c *Converter) Convert(to, from interface{}) (err error) {
	if c.unfold == nil || c.fold == nil {
		c.init()
	}

	defer func() {
		if err != nil {
			c.fold = nil
			c.unfold = nil
		}
	}()

	if err = c.unfold.SetTarget(to); err != nil {
		return err
	}

	defer c.unfold.Reset()
	return c.fold.Fold(from)
}

// Convert transforms the value of from into to, by translating the structure
// from into a set of events (go-structform.Visitor) that can applied to the
// value given by to.
// The operation fails if the values are not compatible (for example trying to
// convert an object into an int), or `to` is no pointer.
func Convert(to, from interface{}) (err error) {
	c := convPool.Get().(*Converter)
	defer convPool.Put(c)
	return c.Convert(to, from)
}
