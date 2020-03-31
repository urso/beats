package journald

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"

	input "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/filebeat/input/v2/statestore"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/registry"
	"github.com/elastic/go-concert/ctxtool"
	"github.com/elastic/go-concert/unison"
	"github.com/urso/sderr"
)

type CursorInputManager struct {
	Logger       *logp.Logger
	Registry     *registry.Registry
	DefaultStore string
	Type         string
	Configure    func(cfg *common.Config) ([]Source, CursorInput, error)

	storeInitOnce  sync.Once
	storeConnector *statestore.Connector
}

type CursorInput interface {
	Test(Source, input.TestContext) error
	Run(input.Context, Source, Cursor, func(beat.Event, interface{}) error) error
}

type Cursor struct {
	res *statestore.Resource
}

type Source interface {
	Name() string
}

type managedInput struct {
	manager *CursorInputManager
	sources []Source
	input   CursorInput
}

func (cim *CursorInputManager) CreateService(fullInputName string) (input.BackgroundService, error) {
	// TODO: create background service cleaning up old states
	return nil, nil
}

func (cim *CursorInputManager) Create(config *common.Config) (input.Input, error) {
	sources, inp, err := cim.Configure(config)
	if err != nil {
		return nil, err
	}

	return &managedInput{cim, sources, inp}, nil
}

func (cim *CursorInputManager) openStore() (*statestore.Store, error) {
	cim.storeInitOnce.Do(func() {
		if cim.Logger == nil {
			cim.Logger = logp.NewLogger("journald")
		}
		cim.storeConnector = statestore.NewConnector(cim.Logger, cim.Registry)
	})
	return cim.storeConnector.Open(cim.DefaultStore)
}

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

	store, err := inp.manager.openStore()
	if err != nil {
		sderr.Wrap(err, "failed to access the state store")
	}
	defer store.Close()

	var grp unison.MultiErrGroup
	for _, source := range inp.sources {
		source := source
		grp.Go(func() (err error) {
			// refine per worker context
			inpCtx := ctx
			inpCtx.ID = ctx.ID + "::" + source.Name()
			inpCtx.Logger = ctx.Logger.With("source", source.Name())

			if err = inp.runSource(inpCtx, store, source, pipeline); err != nil {
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
	store *statestore.Store,
	source Source,
	pipeline beat.PipelineConnector,
) (err error) {
	// Setup error recovery/reporting
	defer func() {
		if v := recover(); v != nil {
			if e, ok := v.(error); ok {
				err = e
			} else {
				err = fmt.Errorf("input panic with: %+v\n%s", v, debug.Stack())
			}
		}
	}()

	// connect to libbeat publisher pipeline
	client, err := pipeline.ConnectWith(beat.ClientConfig{
		CloseRef: ctx.Cancelation,
		Processing: beat.ProcessingConfig{
			DynamicFields: ctx.Metadata,
		},
	})
	if err != nil {
		return err
	}
	defer client.Close()

	// lock resource for exclusive access and create cursor
	cursor, err := inp.lockCursor(ctx, store, source)
	if err != nil {
		return err
	}
	defer cursor.res.Unlock()

	return inp.input.Run(ctx, source, cursor, func(event beat.Event, cursorUpd interface{}) error {
		op, err := cursor.res.UpdateOp(cursorUpd)
		if err != nil {
			return err
		}

		event.Private = op
		client.Publish(event)
		return ctx.Cancelation.Err()
	})
}

func (inp *managedInput) lockCursor(
	ctx input.Context,
	store *statestore.Store,
	source Source,
) (Cursor, error) {
	log := ctx.Logger

	cursorKey := fmt.Sprintf("%v::%v", inp.manager.Type, source.Name())
	res := store.Access(statestore.ResourceKey(cursorKey))
	if !res.TryLock() {
		log.Infof("Resource '%v' currently in use, waiting...", cursorKey)
		err := res.LockContext(ctx.Cancelation)
		if err != nil {
			log.Infof("Input for resource '%v' has been stopped while waiting", cursorKey)
			return Cursor{}, err
		}
	}
	return Cursor{res}, nil
}

func (c Cursor) Unpack(to interface{}) error   { return c.res.Read(to) }
func (c Cursor) Migrate(val interface{}) error { return c.res.Replace(val) }
func (c Cursor) IsNew() bool {
	has, err := c.res.Has()
	return !has || err != nil
}
