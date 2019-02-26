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

package ilm

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path"
	"time"

	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/outputs/elasticsearch"
)

type ilmSupport struct {
	log *logp.Logger

	mode        Mode
	overwrite   bool
	checkExists bool

	alias  Alias
	policy Policy
}

type singlePolicyManager struct {
	*ilmSupport
	client *elasticsearch.Client

	// cached info
	cache infoCache
}

type infoCache struct {
	LastUpdate time.Time
	Enabled    bool
}

var defaultCacheDuration = 5 * time.Minute

// NewDefaultSupport creates an instance of default ILM support implementation.
func NewDefaultSupport(
	log *logp.Logger,
	mode Mode,
	alias Alias,
	policy Policy,
	overwrite, checkExists bool,
) Supporter {
	return &ilmSupport{
		log:         log,
		mode:        mode,
		overwrite:   overwrite,
		checkExists: checkExists,
		alias:       alias,
		policy:      policy,
	}
}

func (s *ilmSupport) Mode() Mode     { return s.mode }
func (s *ilmSupport) Alias() Alias   { return s.alias }
func (s *ilmSupport) Policy() Policy { return s.policy }

func (s *ilmSupport) Manager(client *elasticsearch.Client) Manager {
	return &singlePolicyManager{
		client:     client,
		ilmSupport: s,
	}
}

func (m *singlePolicyManager) Enabled() (bool, error) {
	if m.mode == ModeDisabled {
		return false, nil
	}

	if m.cache.Valid() {
		return m.cache.Enabled, nil
	}

	avail, probe := m.checkILMVersion(m.mode)
	if !avail {
		if m.mode == ModeEnabled {
			ver := m.client.GetVersion()
			return false, fmt.Errorf("Elasticsearch %v does not support ILM", ver.String())
		}
		return false, nil
	}

	if !probe {
		// version potentially supports ILM, but mode + version indicates that we
		// want to disable ILM support.
		return false, nil
	}

	var response struct {
		Features struct {
			ILM struct {
				Available bool `json:"available"`
				Enabled   bool `json:"enabled"`
			} `json:"ilm"`
		} `json:"features"`
	}
	_, err := m.queryFeatures(&response)
	if err != nil {
		return false, err
	}

	avail = response.Features.ILM.Available
	enabled := response.Features.ILM.Enabled

	if !avail {
		if m.mode == ModeEnabled {
			return false, errors.New("ILM is not supported by the Elasticsearch version in use")
		}
		return false, nil
	}

	if !enabled && m.mode == ModeEnabled {
		return false, errors.New("ILM is disabled")
	}

	m.cache.Enabled = enabled
	m.cache.LastUpdate = time.Now()
	return enabled, nil
}

var (
	esMinDefaultILMVesion = common.MustNewVersion("7.0.0")
)

func (m *singlePolicyManager) queryFeatures(to interface{}) (int, error) {
	status, body, err := m.client.Request("GET", "/_xpack", "", nil, nil)
	if status >= 400 || err != nil {
		return status, err
	}

	if to != nil {
		if err := json.Unmarshal(body, to); err != nil {
			return status, err
		}
	}
	return status, nil
}

func (m *singlePolicyManager) checkILMVersion(mode Mode) (avail, probe bool) {
	ver := m.client.GetVersion()
	esMinILMVersion := common.MustNewVersion("6.6.0")
	avail = !ver.LessThan(esMinILMVersion)
	if avail {
		probe = (mode == ModeEnabled) ||
			(mode == ModeAuto && !ver.LessThan(esMinDefaultILMVesion))
	}
	return
}

func (m *singlePolicyManager) EnsureAlias() error {
	path := path.Join("/_alias", m.alias.Name)
	status, _, err := m.client.Request("HEAD", path, "", nil, nil)
	if err != nil && status != 404 {
		return err
	}

	if status == 200 {
		return nil
	}

	// This always assume it's a date pattern by sourrounding it by <...>
	firstIndex := fmt.Sprintf("<%s-%s>", m.alias.Name, m.alias.Pattern)
	firstIndex = url.PathEscape(firstIndex)

	body := common.MapStr{
		"aliases": common.MapStr{
			m.alias.Name: common.MapStr{
				"is_write_index": true,
			},
		},
	}

	// Note: actual aliases are accessible via the index
	status, _, err = m.client.Request("PUT", "/"+firstIndex, "", nil, body)
	if status == 400 {
		return errors.New("unexpected 404")
	} else if err != nil {
		return err
	}

	return nil
}

func (m *singlePolicyManager) EnsurePolicy(overwrite bool) error {
	log := m.log
	overwrite = overwrite || m.overwrite

	exists := true
	if m.checkExists && !overwrite {
		path := path.Join("/_ilm/policy", m.policy.Name)
		status, _, err := m.client.Request("GET", path, "", nil, nil)
		if err != nil && status != 404 {
			return err
		}
		exists = status == 200
	}

	if !exists || overwrite {
		path := path.Join("/_ilm/policy", m.policy.Name)
		_, _, err := m.client.Request("PUT", path, "", nil, m.policy.Body)
		return err
	}

	log.Infof("do not generate ilm policy: exists=%v, overwrite=%v",
		exists, overwrite)
	return nil
}

func (c *infoCache) Valid() bool {
	return !c.LastUpdate.IsZero() && time.Since(c.LastUpdate) < defaultCacheDuration
}
