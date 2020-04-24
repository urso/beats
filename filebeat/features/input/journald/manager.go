package journald

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/urso/sderr"

	input "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/common/acker"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/registry"

	"github.com/elastic/go-concert"
	"github.com/elastic/go-concert/chorus"
	"github.com/elastic/go-concert/ctxtool"
	"github.com/elastic/go-concert/unison"
)

type CursorInputManager struct {
	Logger              *logp.Logger
	Registry            *registry.Registry
	DefaultStore        string
	Type                string
	DefaultCleanTimeout time.Duration
	Configure           func(cfg *common.Config) ([]Source, CursorInput, error)

	initOnce sync.Once

	sessionMu sync.Mutex
	sessions  map[string]*session
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

func (cim *CursorInputManager) Create(config *common.Config) (input.Input, error) {
	cim.initOnce.Do(func() {
		if cim.DefaultCleanTimeout <= 0 {
			cim.DefaultCleanTimeout = 30 * time.Minute
		}
		cim.sessions = map[string]*session{}
	})

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

func (cim *CursorInputManager) accessSession(name string) (*session, error) {
	if name == "" {
		name = cim.DefaultStore
	}

	cim.sessionMu.Lock()
	defer cim.sessionMu.Unlock()

	sess := cim.sessions[name]
	if sess != nil {
		sess.refCount.Retain()
		return sess, nil
	}

	log := cim.Logger.With("store", name)

	store, err := openStore(log, cim.Registry, name, cim.Type)
	if err != nil {
		return nil, err
	}
	defer store.Release()

	closer := chorus.NewCloser(nil)

	store.Retain()
	sess = &session{name: name, log: log, owner: cim, closer: closer, store: store}

	store.Retain()
	cleaner := &cleaner{log: log, closer: closer, store: store}
	go cleaner.run(1 * time.Minute) // TODO: how to configure. This is a global setting, that must not be configured per input

	cim.sessions[name] = sess
	return sess, nil
}

func (cim *CursorInputManager) releaseSession(sess *session, name string, counter *concert.RefCount) bool {
	cim.sessionMu.Lock()
	done := counter.Release()
	if done {
		delete(cim.sessions, name)
	}
	cim.sessionMu.Unlock()
	return done
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

	session, err := inp.manager.accessSession("")
	if err != nil {
		sderr.Wrap(err, "failed to access the state store")
	}
	defer session.Release()

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
	resource, err := session.Lock(ctx, fmt.Sprintf("%v::%v", inp.manager.Type, source.Name()))
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
