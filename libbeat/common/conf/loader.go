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
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/elastic/beats/libbeat/common/file"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/go-ucfg/cfgutil"
	"github.com/elastic/go-ucfg/yaml"
)

type Loader struct {
	log              *logp.Logger
	home             string
	checkPermissions bool
}

type LoaderOption interface {
	apply(l *Loader)
}

type LoaderPathSettings struct {
	Home             string
	CheckPermissions bool
}

type funcOption func(*Loader)

func NewLoader(log *logp.Logger, opts ...LoaderOption) *Loader {
	const selector = "config"

	if log == nil {
		log = logp.NewLogger(selector)
	} else {
		log = log.Named(selector)
	}

	l := &Loader{
		log:              log,
		home:             ".",
		checkPermissions: false,
	}
	return l
}

func (l *Loader) Load(path string) (*Config, error) {
	cfg, err := l.loadFile(path)
	if err == nil {
		// TODO: debug log
	}
	return cfg, err
}

func (l *Loader) LoadAll(paths ...string) (*Config, error) {
	merger := cfgutil.NewCollector(nil, configOpts...)
	for _, path := range paths {
		cfg, err := l.loadFile(path)
		if err := merger.Add(cfg.access(), err); err != nil {
			return nil, err
		}
	}

	cfg := fromConfig(merger.Config())
	// TODO: debug log
	return cfg, nil
}

func (l *Loader) loadFile(path string) (*Config, error) {
	if !filepath.IsAbs(path) {
		path = filepath.Join(l.home, path)
	}

	if l.checkPermissions {
		if err := checkExclusiveWritePerms(path); err != nil {
			return nil, err
		}
	}

	c, err := yaml.NewConfigWithFile(path, configOpts...)
	return fromConfig(c), err
}

func (fn funcOption) apply(l *Loader) { fn(l) }

func (s LoaderPathSettings) apply(l *Loader) {
	if s.Home != "" {
		l.home = s.Home
	}
	l.checkPermissions = s.CheckPermissions
}

func checkExclusiveWritePerms(name string) error {
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
