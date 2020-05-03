package exclinput

import (
	"time"

	"github.com/urso/sderr"

	input "github.com/elastic/beats/v7/filebeat/input/v2"
	v2 "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/statestore"

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

type Source interface {
	Name() string
}

type StateStore interface {
	Access() (*statestore.Store, error)
	CleanupInterval() time.Duration
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
