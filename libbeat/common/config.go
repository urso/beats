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
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/elastic/beats/libbeat/common/conf"
	"github.com/elastic/beats/libbeat/common/file"
	"github.com/elastic/beats/libbeat/common/mapstrings"
	"github.com/elastic/beats/libbeat/logp"
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

const (
	selectorConfig             = "config"
	selectorConfigWithPassword = "config-with-passwords"
)

// make hasSelector and configDebugf available for unit testing
var hasSelector = logp.HasSelector
var configDebugf = logp.Debug

func NewConfig() *Config {
	return conf.NewConfig()
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

	debugStr := DebugString(c, filtered)
	if debugStr != "" {
		configDebugf(selector, "%s\n%s", fmt.Sprintf(msg, params...), debugStr)
	}
}

// DebugString prints a human readable representation of the underlying config using
// JSON formatting.
func DebugString(c *Config, filterPrivate bool) string {
	var bufs []string

	if c.IsDict() {
		var content map[string]interface{}
		if err := c.Unpack(&content); err != nil {
			return fmt.Sprintf("<config error> %v", err)
		}
		if filterPrivate {
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
		if filterPrivate {
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

// OwnerHasExclusiveWritePerms asserts that the current user or root is the
// owner of the config file and that the config file is (at most) writable by
// the owner or root (e.g. group and other cannot have write access).
func OwnerHasExclusiveWritePerms(name string) error {
	if runtime.GOOS == "windows" {
		return nil
	}

	info, err := file.Stat(name)
	if err != nil {
		return err
	}

	euid := os.Geteuid()
	fileUID, _ := info.UID()
	perm := info.Mode().Perm()

	if fileUID != 0 && euid != fileUID {
		return fmt.Errorf(`config file ("%v") must be owned by the user identifier `+
			`(uid=%v) or root`, name, euid)
	}

	// Test if group or other have write permissions.
	if perm&0022 > 0 {
		nameAbs, err := filepath.Abs(name)
		if err != nil {
			nameAbs = name
		}
		return fmt.Errorf(`config file ("%v") can only be writable by the `+
			`owner but the permissions are "%v" (to fix the permissions use: `+
			`'chmod go-w %v')`,
			name, perm, nameAbs)
	}

	return nil
}
