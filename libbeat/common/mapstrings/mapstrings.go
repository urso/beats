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

package mapstrings

import (
	"reflect"
	"strings"

	"github.com/pkg/errors"
)

type path struct {
	str string
	sep rune
}

// ErrEmptyPath indicates that the given path was an empty string.
var ErrEmptyPath = errors.New("no path given")

var defaultRedactSet = makeSet(
	"password",
	"passphrase",
	"key_passphrase",
	"pass",
	"proxy_url",
	"url",
	"urls",
	"host",
	"hosts",
)

// HasKey checks if the top-level key exists in the map:
func HasKey(m map[string]interface{}, key string) bool {
	_, exists := m[key]
	return exists
}

// HasPath returns true if the path given by key exists. The map is searched
// recursively based on the elements the keyPath is split by sep.
func HasPath(m map[string]interface{}, keyPath string, sep rune) bool {
	_, exists := walk(m, &path{keyPath, sep})
	return exists
}

// Get finds a value in the map for the given path.
func Get(m map[string]interface{}, keyPath string, sep rune) (interface{}, bool) {
	return walk(m, &path{keyPath, sep})
}

func walk(m map[string]interface{}, p *path) (interface{}, bool) {
	if !p.more() {
		return nil, false
	}

	for {
		// fast path, check if path is present as is
		if v, exists := m[p.str]; exists {
			return v, true
		}

		elem := p.next()
		if !p.more() { // we did already try to match
			return nil, false
		}

		v, exists := m[elem]
		if !exists {
			return nil, false
		}

		var ok bool
		m, ok = toMap(v)
		if !ok {
			return nil, false
		}
	}
}

// Put adds the value for a keyPath to the map. The old value is replaced and
// returned. If `keep` is given then Put will copy the original value into a
// values named `keep` instead of overwriting intermediate maps.
func Put(
	m map[string]interface{},
	keyPath string, sep rune,
	val interface{},
	keepKey string,
) (interface{}, error) {
	path := &path{keyPath, sep}
	if !path.more() {
		return nil, ErrEmptyPath
	}

	// 1. iterate through the map until we reach the final element in our path
	//    or no more child-map is present.
	var (
		key string

		// oldMap and oldVal is used to remember the position in the path
		// that has been overwritten with a new map
		oldMap map[string]interface{}
		oldVal interface{}
	)
	for {
		key = path.next()
		if !path.more() {
			break
		}

		v, ok := m[key]
		if !ok {
			break // Key does not exist. We need to add maps recursively.
		}

		tmp, ok := toMap(v)
		if !ok {
			// Key exists but is no map. We need replace the key with a new map and
			// recursively add intermaediate maps in step 2.
			// Remember the old map/value we are going to overwrite
			oldMap, oldVal = m, v
			break
		}

		// next
		m = tmp
	}

	// 2. recursively insert maps until we reach the final element in our path
	for path.more() {
		tmp := make(map[string]interface{}, 1)
		m[key] = tmp
		m, key = tmp, path.next()
	}

	// 3. finally insert our new value into the map
	//    If the old value is a map and keepKey is set, then we will add
	//    the new value under the old map using keepKey
	if keepKey == "" {
		old := m[key]
		m[key] = val
		return old, nil
	}
	old, hasOld := m[key]
	if m, ok := toMap(old); ok {
		m[keepKey] = val
		return nil, nil
	}

	// 4. overwrite and add original value
	m[key] = val
	if oldMap != nil {
		// oldMap and oldVal is set if we did replace a primitive value with a new map
		oldMap[keepKey] = oldVal
	} else if hasOld {
		// old value was been found in the same map. Only reinsert it if
		m[keepKey] = old
	}
	return old, nil
}

// Delete recursively deletes the value associated with keyPath. Intermediate
// maps are deleted of they become empty.
func Delete(m map[string]interface{}, keyPath string, sep rune) (interface{}, bool) {
	path := &path{keyPath, sep}
	if !path.more() {
		return nil, false
	}

	return deletePath(m, path)
}

func deletePath(m map[string]interface{}, path *path) (interface{}, bool) {
	key := path.next()
	v, exists := m[key]
	if !exists {
		return nil, false
	}

	if !path.more() {
		delete(m, key)
		return v, true
	}

	tmp, ok := toMap(v)
	if !ok {
		return nil, false
	}

	delValue, ok := deletePath(tmp, path)
	if !ok {
		return delValue, ok
	}

	if len(tmp) == 0 {
		delete(m, key)
	}
	return delValue, true
}

// Flatten recursively flattens the given map, such that keys in child maps are
// combined.  The resulting map does not contain a map itself anymore.
func Flatten(m map[string]interface{}, sep string) map[string]interface{} {
	return flatten("", sep, m, map[string]interface{}{})
}

func flatten(prefix, sep string, in, out map[string]interface{}) map[string]interface{} {
	for k, v := range in {
		var fullKey string
		if prefix == "" {
			fullKey = k
		} else {
			fullKey = prefix + sep + k
		}

		if m, ok := toMap(v); ok {
			flatten(fullKey, sep, m, out)
		} else {
			out[fullKey] = v
		}
	}
	return out
}

// Keys returns an array of top-level keys.
func Keys(m map[string]interface{}) []string {
	i, keys := 0, make([]string, len(m))
	for k := range m {
		keys[i] = k
		i++
	}
	return keys
}

// Values returns an array of top-level values.
func Values(m map[string]interface{}) []interface{} {
	i, vals := 0, make([]interface{}, len(m))
	for _, v := range m {
		vals[i] = v
		i++
	}
	return vals
}

// Split create a slice of keys and values.
func Split(m map[string]interface{}) ([]string, []interface{}) {
	i, keys, vals := 0, make([]string, len(m)), make([]interface{}, len(m))
	for k, v := range m {
		keys[i] = k
		vals[i] = v
		i++
	}
	return keys, vals
}

// Copy creates a shallow copy of the map, only copying top-level keys.
func Copy(m map[string]interface{}) map[string]interface{} {
	res := make(map[string]interface{}, len(m))
	for k, v := range m {
		res[k] = v
	}
	return res
}

// DeepCopy recursively copies the map into a new one.
func DeepCopy(m map[string]interface{}) map[string]interface{} {
	res := make(map[string]interface{}, len(m))
	for k, v := range m {
		res[k] = deepCopy(v)
	}
	return res
}

func deepCopyArr(arr []interface{}) []interface{} {
	cpy := make([]interface{}, len(arr))
	for i, v := range arr {
		cpy[i] = deepCopy(v)
	}
	return cpy
}

func deepCopy(v interface{}) interface{} {
	if m, ok := toMap(v); ok {
		return DeepCopy(m)
	} else if a, ok := toArr(v); ok {
		return deepCopyArr(a)
	}
	return v
}

// Redact replaces all keys that are available in keySet with overwrite.
// If keySet is empty, a default set will be used redacting common keys like
// "password", "passphrase", "pass", "host", and others.
func Redact(m interface{}, overwrite string, keySet map[string]struct{}) interface{} {
	if len(keySet) == 0 {
		keySet = defaultRedactSet
	}
	if overwrite == "" {
		overwrite = "xxxxx"
	}
	tmp := deepCopy(m)
	redactVal(tmp, overwrite, keySet)
	return tmp
}

func redactVal(val interface{}, overwrite string, keySet map[string]struct{}) {
	if m, ok := toMap(val); ok {
		redactMap(m, overwrite, keySet)
	} else if a, ok := toArr(val); ok {
		redactArr(a, overwrite, keySet)
	}
}

func redactMap(m map[string]interface{}, overwrite string, keySet map[string]struct{}) {
	for k, v := range m {
		if _, exists := keySet[k]; exists {
			redactOverwrite(m, k, v, overwrite)
		} else {
			redactVal(v, overwrite, keySet)
		}
	}
}

func redactArr(arr []interface{}, overwrite string, keySet map[string]struct{}) {
	for _, v := range arr {
		redactVal(v, overwrite, keySet)
	}
}

func redactOverwrite(m map[string]interface{}, k string, val interface{}, overwrite string) {
	if arr, ok := toArr(val); ok {
		for i := range arr {
			arr[i] = overwrite
		}
		return
	}
	m[k] = overwrite
}

var tMapStrIfc = reflect.TypeOf((map[string]interface{})(nil))
var tArrIfc = reflect.TypeOf(([]interface{})(nil))

func toMap(val interface{}) (map[string]interface{}, bool) {
	if m, ok := val.(map[string]interface{}); ok {
		return m, ok
	}

	v := reflect.ValueOf(val)
	if v.Type().ConvertibleTo(tMapStrIfc) {
		return v.Convert(tMapStrIfc).Interface().(map[string]interface{}), true
	}

	return nil, false
}

func toArr(val interface{}) ([]interface{}, bool) {
	if a, ok := val.([]interface{}); ok {
		return a, ok
	}

	v := reflect.ValueOf(val)
	if v.Type().ConvertibleTo(tArrIfc) {
		return v.Convert(tArrIfc).Interface().([]interface{}), true
	}

	return nil, false
}

func (p *path) more() bool {
	return len(p.str) > 0
}

func (p *path) next() string {
	idx := strings.IndexRune(p.str, p.sep)
	if idx < 0 {
		key := string(p.sep)
		p.str = ""
		return key
	}

	key := string(p.str[:idx])
	p.str = p.str[idx+1:]
	return key
}

func makeSet(strs ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(strs))
	for _, str := range strs {
		m[str] = struct{}{}
	}
	return m
}
