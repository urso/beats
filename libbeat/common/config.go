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

package common

import (
	"flag"
	"os"

	"github.com/elastic/beats/libbeat/common/conf"
	ucfg "github.com/elastic/go-ucfg"
)

var flagStrictPerms = flag.Bool("strict.perms", true, "Strict permission checking on config files")

// IsStrictPerms returns true if strict permission checking on config files is
// enabled.
func IsStrictPerms() bool {
	if !*flagStrictPerms || os.Getenv("BEAT_STRICT_PERMS") == "false" {
		return false
	}
	return true
}

// Config object to store hierarchical configurations into.
// See https://godoc.org/github.com/elastic/go-ucfg#Config
type Config = conf.Config

// ConfigNamespace storing at most one configuration section by name and sub-section.
type ConfigNamespace = conf.Namespace

// NewConfig creates a new empty configuration object.
func NewConfig() *Config {
	return conf.New()
}

// NewConfigFrom creates a new Config object from the given input.
// From can be any kind of structured data (struct, map, array, slice).
//
// If from is a string, the contents is treated like raw YAML input. The string
// will be parsed and a structure config object is build from the parsed
// result.
func NewConfigFrom(from interface{}) (*Config, error) {
	return conf.NewFrom(from)
}

// MustNewConfigFrom creates a new Config object from the given input.
// From can be any kind of structured data (struct, map, array, slice).
//
// If from is a string, the contents is treated like raw YAML input. The string
// will be parsed and a structure config object is build from the parsed
// result.
//
// MustNewConfigFrom panics if an error occurs.
func MustNewConfigFrom(from interface{}) *Config {
	return conf.MustNewFrom(from)
}

// MergeConfigs combines multiple configurations into a single one.
func MergeConfigs(cfgs ...*Config) (*Config, error) {
	return conf.Combine(cfgs...)
}

func NewConfigWithYAML(in []byte, source string) (*Config, error) {
	return conf.NewFromYAML(in, source)
}

// OverwriteConfigOpts allow to change the globally set config option
func OverwriteConfigOpts(options []ucfg.Option) {
	conf.OverwriteConfigOpts(options)
}

func LoadFile(path string) (*Config, error) {
	l := conf.NewLoader(nil, conf.LoaderPathSettings{
		CheckPermissions: IsStrictPerms(),
	})
	return l.Load(path)
}

func LoadFiles(paths ...string) (*Config, error) {
	l := conf.NewLoader(nil, conf.LoaderPathSettings{
		CheckPermissions: IsStrictPerms(),
	})
	return l.LoadAll(paths...)
}

// DebugString prints a human readable representation of the underlying config using
// JSON formatting.
func DebugString(c *Config, filterPrivate bool) string {
	return c.ToString(filterPrivate)
}
