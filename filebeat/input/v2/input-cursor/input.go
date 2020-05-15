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

package cursor

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	input "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common/acker"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/go-concert/ctxtool"
	"github.com/elastic/go-concert/unison"
	"github.com/urso/sderr"
)

type Input interface {
	Name() string
	Test(Source, input.TestContext) error
	Run(input.Context, Source, Cursor, Publisher) error
}

type Cursor struct {
	store    *store
	resource *resource
}

type managedInput struct {
	manager      *InputManager
	userID       string
	sources      []Source
	input        Input
	cleanTimeout time.Duration
}

func (inp *managedInput) Name() string { return inp.input.Name() }

func (inp *managedInput) Test(ctx input.TestContext) error {
	var grp unison.MultiErrGroup
	for _, source := range inp.sources {
		source := source
		grp.Go(func() error { return inp.input.Test(source, ctx) })
	}
	if errs := grp.Wait(); len(errs) > 0 {
		sderr.WrapAll(errs, "input tests failed")
	}
	return nil
}

func (inp *managedInput) Run(
	ctx input.Context,
	pipeline beat.PipelineConnector,
) (err error) {
	// Setup cancellation using a custom cancel context. All workers will be
	// stopped if one failed badly.
	cancelCtx, cancel := context.WithCancel(ctxtool.FromCanceller(ctx.Cancelation))
	defer cancel()
	ctx.Cancelation = cancelCtx

	var grp unison.MultiErrGroup
	for _, source := range inp.sources {
		source := source
		grp.Go(func() (err error) {
			// refine per worker context
			inpCtx := ctx
			inpCtx.ID = ctx.ID + "::" + source.Name()
			inpCtx.Logger = ctx.Logger.With("source", source.Name())

			if err = inp.runSource(inpCtx, inp.manager.store, source, pipeline); err != nil {
				cancel()
			}
			return err
		})
	}

	if errs := grp.Wait(); len(errs) > 0 {
		sderr.WrapAll(errs, "input %v failed", ctx.ID)
	}
	return nil
}

func (inp *managedInput) runSource(
	ctx input.Context,
	store *store,
	source Source,
	pipeline beat.PipelineConnector,
) (err error) {
	// Setup error recovery/reporting
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf("input panic with: %+v\n%s", v, debug.Stack())
			ctx.Logger.Errorf("Input crashed with: %+v", err)
		}
	}()

	// connect to libbeat publisher pipeline
	client, err := pipeline.ConnectWith(beat.ClientConfig{
		CloseRef: ctx.Cancelation,
		Processing: beat.ProcessingConfig{
			DynamicFields: ctx.Metadata,
		},
		ACKHandler: newInputACKHandler(ctx.Logger),
	})
	if err != nil {
		return err
	}
	defer client.Close()

	// lock resource for exclusive access and create cursor
	resourceKey := inp.createSourceID(source)
	resource, err := inp.manager.lock(ctx, resourceKey)
	if err != nil {
		return err
	}
	defer resource.Unlock()

	// update clean timeout. If the resource is 'new' we will insert it into the registry now.
	if resource.stored == false || inp.cleanTimeout != resource.state.Internal.TTL {
		resource.state.Internal.TTL = inp.cleanTimeout
		store.UpdateInternal(resource)
	}

	cursor := Cursor{store: store, resource: resource}
	publisher := &cursorPublisher{ctx: &ctx, client: client, cursor: &cursor}
	return inp.input.Run(ctx, source, cursor, publisher)
}

func (inp *managedInput) createSourceID(s Source) string {
	if inp.userID != "" {
		return fmt.Sprintf("%v::%v::%v", inp.manager.Type, inp.userID, s.Name())
	}
	return fmt.Sprintf("%v::%v", inp.manager.Type, s.Name())
}

func (c Cursor) IsNew() bool { return c.resource.IsNew() }

func (c Cursor) Unpack(to interface{}) error {
	if c.IsNew() {
		return nil
	}
	return c.resource.UnpackCursor(to)
}

func (c Cursor) Migrate(val interface{}) error {
	return c.store.Migrate(c.resource, val)
}

func newInputACKHandler(log *logp.Logger) beat.ACKer {
	return acker.EventPrivateReporter(func(acked int, private []interface{}) {
		for i := len(private) - 1; i >= 0; i-- {
			current := private[i]
			if current == nil {
				continue
			}

			op, ok := current.(*updateOp)
			if !ok {
				continue
			}

			op.Execute()
			return
		}
	})
}
