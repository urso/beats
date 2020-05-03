package exclinput

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

type CursorInput interface {
	Name() string
	Test(Source, input.TestContext) error
	Run(input.Context, Source, Cursor, Publisher) error
}

type Cursor struct {
	session  *session
	resource *resource
}

type managedInput struct {
	manager      *CursorInputManager
	sources      []Source
	input        CursorInput
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

	session := inp.manager.session
	if err != nil {
		sderr.Wrap(err, "failed to access the state store")
	}

	var grp unison.MultiErrGroup
	for _, source := range inp.sources {
		source := source
		grp.Go(func() (err error) {
			// refine per worker context
			inpCtx := ctx
			inpCtx.ID = ctx.ID + "::" + source.Name()
			inpCtx.Logger = ctx.Logger.With("source", source.Name())

			if err = inp.runSource(inpCtx, session, source, pipeline); err != nil {
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
	session *session,
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
	resource, err := inp.manager.lock(ctx, fmt.Sprintf("%v::%v", inp.manager.Type, source.Name()))
	if err != nil {
		return err
	}
	defer resource.Unlock()

	// update clean timeout. If the resource is 'new' we will insert it into the registry now.
	if resource.stored == false || inp.cleanTimeout != resource.state.Internal.TTL {
		resource.state.Internal.TTL = inp.cleanTimeout
		session.store.UpdateInternal(resource)
	}

	cursor := Cursor{session: session, resource: resource}
	publisher := &cursorPublisher{ctx: &ctx, client: client, cursor: &cursor}
	return inp.input.Run(ctx, source, cursor, publisher)
}

func (c Cursor) IsNew() bool { return c.resource.IsNew() }

func (c Cursor) Unpack(to interface{}) error {
	if c.IsNew() {
		return nil
	}
	return c.resource.UnpackCursor(to)
}

func (c Cursor) Migrate(val interface{}) error {
	return c.session.store.Migrate(c.resource, val)
}

func newInputACKHandler(log *logp.Logger) beat.ACKer {
	return acker.LastEventPrivateReporter(func(acked int, private interface{}) {
		op, ok := private.(*updateOp)
		if !ok {
			log.Errorf("Wrong event type ACKed")
		} else {
			op.Execute()
		}
	})
}
