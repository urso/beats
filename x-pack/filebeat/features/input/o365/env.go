// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package o365

import (
	"time"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/x-pack/filebeat/input/o365audit/poll"
	"github.com/joeshaw/multierror"
	"github.com/pkg/errors"
)

type apiEnvironment struct {
	TenantID    string
	ContentType string
	Config      APIConfig
	Callback    func(event beat.Event, cursor interface{}) error
	Logger      *logp.Logger
	Clock       func() time.Time
}

const fieldsPrefix = pluginName

var errTerminated = errors.New("terminated due to output closed")

// Report returns an action that produces a beat.Event from the given object.
func (env apiEnvironment) Report(doc common.MapStr, cp checkpoint) poll.Action {
	return func(poll.Enqueuer) error {
		return env.Callback(env.toBeatEvent(doc), cp)
	}
}

// ReportAPIError returns an action that produces a beat.Event from an API error.
func (env apiEnvironment) ReportAPIError(err apiError) poll.Action {
	return func(poll.Enqueuer) error {
		return env.Callback(err.ToBeatEvent(), nil)
	}
}

func (env apiEnvironment) toBeatEvent(doc common.MapStr) beat.Event {
	var errs multierror.Errors
	ts, err := getDateKey(doc, "CreationTime", apiDateFormats)
	if err != nil {
		ts = time.Now()
		errs = append(errs, errors.Wrap(err, "failed parsing CreationTime"))
	}
	b := beat.Event{
		Timestamp: ts,
		Fields: common.MapStr{
			fieldsPrefix: doc,
		},
	}
	if env.Config.SetIDFromAuditRecord {
		if id, err := getString(doc, "Id"); err == nil && len(id) > 0 {
			b.SetID(id)
		}
	}
	if len(errs) > 0 {
		msgs := make([]string, len(errs))
		for idx, e := range errs {
			msgs[idx] = e.Error()
		}
		b.PutValue("error.message", msgs)
	}
	return b
}
