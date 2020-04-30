package exclinput

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/urso/sderr"

	input "github.com/elastic/beats/v7/filebeat/input/v2"
	v2 "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/common/acker"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/statestore"

	"github.com/elastic/go-concert/ctxtool"
	"github.com/elastic/go-concert/unison"
)

type CursorInputManager struct {
	Logger              *logp.Logger
	StateStore          StateStore
	Type                string
	DefaultCleanTimeout time.Duration
	Configure           func(cfg *common.Config) ([]Source, CursorInput, error)

	session *session
	store   *store
}

type StateStore interface {
	Access() (*statestore.Store, error)
	CleanupInterval() time.Duration
}

type CursorInput interface {
	Name() string
	Test(Source, input.TestContext) error
	Run(input.Context, Source, Cursor, Publisher) error
}

type Publisher interface {
	Publish(event beat.Event, cursor interface{}) error
}

type Cursor struct {
	session  *session
	resource *resource
}

type Source interface {
	Name() string
}

type managedInput struct {
	manager      *CursorInputManager
	sources      []Source
	input        CursorInput
	cleanTimeout time.Duration
}

type cursorPublisher struct {
	ctx    *input.Context
	client beat.Client
	cursor *Cursor
}

func (cim *CursorInputManager) init() error {
	if cim.DefaultCleanTimeout <= 0 {
		cim.DefaultCleanTimeout = 30 * time.Minute
	}

	log := cim.Logger.With("input_type", cim.Type)
	store, err := openStore(log, cim.StateStore, cim.Type)
	if err != nil {
		return err
	}

	cim.session = newSession(store)
	cim.store = store

	return nil
}

func (cim *CursorInputManager) Init(group unison.Group, mode v2.Mode) error {
	if mode != v2.ModeRun {
		return nil
	}

	if err := cim.init(); err != nil {
		return err
	}

	log := cim.Logger.With("input_type", cim.Type)

	store := cim.store
	cleaner := &cleaner{log: log}
	store.Retain()
	err := group.Go(func(canceler unison.Canceler) error {
		defer cim.shutdown()
		defer store.Release()
		interval := cim.StateStore.CleanupInterval()
		if interval <= 0 {
			interval = 5 * time.Minute
		}
		cleaner.run(canceler, store, interval)
		return nil
	})
	if err != nil {
		store.Release()
		cim.shutdown()
		return sderr.Wrap(err, "Can not start registry cleanup process")
	}

	return nil
}

func (cim *CursorInputManager) shutdown() {
	cim.session.Close()
}

func (cim *CursorInputManager) Create(config *common.Config) (input.Input, error) {
	if err := cim.init(); err != nil {
		return nil, err
	}

	settings := struct {
		CleanTimeout time.Duration `config:"clean_timeout"`
	}{CleanTimeout: cim.DefaultCleanTimeout}
	if err := config.Unpack(&settings); err != nil {
		return nil, err
	}

	sources, inp, err := cim.Configure(config)
	if err != nil {
		return nil, err
	}

	return &managedInput{
		manager:      cim,
		sources:      sources,
		input:        inp,
		cleanTimeout: settings.CleanTimeout,
	}, nil
}

// Lock locks a key for exclusive access and returns an resource that can be used to modify
// the cursor state and unlock the key.
func (cim *CursorInputManager) lock(ctx input.Context, key string) (*resource, error) {
	log := ctx.Logger

	resource := cim.store.Find(key, true)
	if !resource.lock.TryLock() {
		log.Infof("Resource '%v' currently in use, waiting...", key)
		err := resource.lock.LockContext(ctx.Cancelation)
		if err != nil {
			log.Infof("Input for resource '%v' has been stopped while waiting", key)
			return nil, err
		}
	}
	return resource, nil
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
		ACKHandler: newInputACKHandler(ctx.Logger, inp.manager),
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

func (c *cursorPublisher) Publish(event beat.Event, cursorUpdate interface{}) error {
	op, err := c.cursor.session.CreateUpdateOp(c.cursor.resource, cursorUpdate)
	if err != nil {
		return err
	}

	event.Private = op
	c.client.Publish(event)
	return c.ctx.Cancelation.Err()
}

func newInputACKHandler(log *logp.Logger, manager *CursorInputManager) beat.ACKer {
	return acker.LastEventPrivateReporter(func(acked int, private interface{}) {
		op, ok := private.(*updateOp)
		if !ok {
			log.Errorf("Wrong event type ACKed")
		} else {
			op.Execute()
		}
	})
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
