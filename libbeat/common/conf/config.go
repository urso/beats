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

package conf

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/elastic/beats/libbeat/common/mapstrings"
	ucfg "github.com/elastic/go-ucfg"
	"github.com/elastic/go-ucfg/yaml"
)

// Config object to store hierarchical configurations into.
// See https://godoc.org/github.com/elastic/go-ucfg#Config
type Config ucfg.Config

// fromConfig casts a *ucfg.Config to (*Config)
func fromConfig(in *ucfg.Config) *Config {
	return (*Config)(in)
}

func New() *Config {
	return fromConfig(ucfg.New())
}

// NewFrom creates a new Config object from the given input.
// From can be any kind of structured data (struct, map, array, slice).
//
// If from is a string, the contents is treated like raw YAML input. The string
// will be parsed and a structure config object is build from the parsed
// result.
func NewFrom(from interface{}) (*Config, error) {
	if str, ok := from.(string); ok {
		c, err := yaml.NewConfig([]byte(str), configOpts...)
		return fromConfig(c), err
	}

	c, err := ucfg.NewFrom(from, configOpts...)
	return fromConfig(c), err
}

// MustNewFrom creates a new Config object from the given input.
// From can be any kind of structured data (struct, map, array, slice).
//
// If from is a string, the contents is treated like raw YAML input. The string
// will be parsed and a structure config object is build from the parsed
// result.
//
// MustNewFrom panics if an error occurs.
func MustNewFrom(from interface{}) *Config {
	cfg, err := NewFrom(from)
	if err != nil {
		panic(err)
	}
	return cfg
}

func NewFromYAML(in []byte, source string) (*Config, error) {
	opts := append(
		[]ucfg.Option{
			ucfg.MetaData(ucfg.Meta{Source: source}),
		},
		configOpts...,
	)
	c, err := yaml.NewConfig(in, opts...)
	return fromConfig(c), err
}

// Enabled return the configured enabled value or true by default.
func (c *Config) Enabled() bool {
	testEnabled := struct {
		Enabled bool `config:"enabled"`
	}{true}

	if c == nil {
		return false
	}
	if err := c.Unpack(&testEnabled); err != nil {
		// if unpacking fails, expect 'enabled' being set to default value
		return true
	}
	return testEnabled.Enabled
}

func (c *Config) PrintDebugf(msg string, params ...interface{}) {
	selector := selectorConfigWithPassword
	filtered := false
	if !hasSelector(selector) {
		selector = selectorConfig
		filtered = true

		if !hasSelector(selector) {
			return
		}
	}

	debugStr := c.ToString(filtered)
	if debugStr != "" {
		configDebugf(selector, "%s\n%s", fmt.Sprintf(msg, params...), debugStr)
	}
}

func (c *Config) ToString(redact bool) string {
	var bufs []string

	if c.IsDict() {
		var content map[string]interface{}
		if err := c.Unpack(&content); err != nil {
			return fmt.Sprintf("<config error> %v", err)
		}
		if redact {
			content = mapstrings.Redact(content, "", nil).(map[string]interface{})
		}
		j, _ := json.MarshalIndent(content, "", "  ")
		bufs = append(bufs, string(j))
	}
	if c.IsArray() {
		var content []interface{}
		if err := c.Unpack(&content); err != nil {
			return fmt.Sprintf("<config error> %v", err)
		}
		if redact {
			content = mapstrings.Redact(content, "", nil).([]interface{})
		}
		j, _ := json.MarshalIndent(content, "", "  ")
		bufs = append(bufs, string(j))
	}

	if len(bufs) == 0 {
		return ""
	}
	return strings.Join(bufs, "\n")
}

func (c *Config) Merge(from interface{}) error {
	return c.access().Merge(from, configOpts...)
}

func (c *Config) Unpack(to interface{}) error {
	return c.access().Unpack(to, configOpts...)
}

func (c *Config) Path() string {
	return c.access().Path(".")
}

func (c *Config) PathOf(field string) string {
	return c.access().PathOf(field, ".")
}

func (c *Config) Remove(name string, idx int) (bool, error) {
	return c.access().Remove(name, idx, configOpts...)
}

func (c *Config) Has(name string, idx int) (bool, error) {
	return c.access().Has(name, idx, configOpts...)
}

func (c *Config) HasField(name string) bool {
	return c.access().HasField(name)
}

func (c *Config) CountField(name string) (int, error) {
	return c.access().CountField(name)
}

func (c *Config) Bool(name string, idx int) (bool, error) {
	return c.access().Bool(name, idx, configOpts...)
}

func (c *Config) String(name string, idx int) (string, error) {
	return c.access().String(name, idx, configOpts...)
}

func (c *Config) Int(name string, idx int) (int64, error) {
	return c.access().Int(name, idx, configOpts...)
}

func (c *Config) Float(name string, idx int) (float64, error) {
	return c.access().Float(name, idx, configOpts...)
}

func (c *Config) Child(name string, idx int) (*Config, error) {
	sub, err := c.access().Child(name, idx, configOpts...)
	return fromConfig(sub), err
}

func (c *Config) SetBool(name string, idx int, value bool) error {
	return c.access().SetBool(name, idx, value, configOpts...)
}

func (c *Config) SetInt(name string, idx int, value int64) error {
	return c.access().SetInt(name, idx, value, configOpts...)
}

func (c *Config) SetFloat(name string, idx int, value float64) error {
	return c.access().SetFloat(name, idx, value, configOpts...)
}

func (c *Config) SetString(name string, idx int, value string) error {
	return c.access().SetString(name, idx, value, configOpts...)
}

func (c *Config) SetChild(name string, idx int, value *Config) error {
	return c.access().SetChild(name, idx, value.access(), configOpts...)
}

func (c *Config) IsDict() bool {
	return c.access().IsDict()
}

func (c *Config) IsArray() bool {
	return c.access().IsArray()
}

func (c *Config) GetFields() []string {
	return c.access().GetFields()
}

func (c *Config) access() *ucfg.Config {
	return (*ucfg.Config)(c)
}
