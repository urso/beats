package urlcollect

import (
	"fmt"
	"net/url"

	v2 "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/filebeat/input/v2/statestore"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/registry"
)

// InputManager controls the execution of active inputs.
type InputManager struct {
	log      *logp.Logger
	registry *registry.Registry

	storeConnector *statestore.Connector
	defaultStore   string
}

type managedInput struct {
	client beat.Client
	run    InputFunc
}

type managedCursor struct {
	res *statestore.Resource
}

type managedPublisher struct {
	client   beat.Client
	canceler v2.Canceler
	res      *statestore.Resource
}

func NewInputManager(log *logp.Logger, registry *registry.Registry, defaultStore string) *InputManager {
	required(registry != nil, "no registry")
	required(log != nil, "no logger")

	log = log.Named("urlcollect")
	return &InputManager{
		log:            log,
		registry:       registry,
		storeConnector: statestore.NewConnector(log, registry),
		defaultStore:   defaultStore,
	}
}

func (m *InputManager) run(
	ctx v2.Context,
	pluginName string,
	url *url.URL,
	worker managedInput,
) error {
	store, err := m.storeConnector.Open(m.defaultStore)
	if err != nil {
		return err
	}

	cursorKey := fmt.Sprintf("urlcollect::%v::%v", pluginName, url)
	res := store.Access(statestore.ResourceKey(cursorKey))
	if !res.TryLock() {
		m.log.Infof("Resource '%v' currently in use, waiting...", cursorKey)
		err := res.LockContext(ctx.Cancelation)
		if err != nil {
			m.log.Infof("Input for resource '%v' has been stopped while waiting", cursorKey)
			return err
		}
	}

	defer res.Unlock()
	m.log.Infof("Processing '%v' starting")
	defer m.log.Infof("Processing '%v' stopped")

	urlCtx := Context{
		ID:          ctx.ID,
		Agent:       ctx.Agent,
		Logger:      ctx.Logger,
		Cancelation: ctx.Cancelation,
		Publisher: &managedPublisher{
			client:   worker.client,
			canceler: ctx.Cancelation,
			res:      res,
		},
	}

	cursor := &managedCursor{res: res}
	return worker.run(urlCtx, url, cursor)
}

func (c *managedCursor) IsNew() bool {
	has, err := c.res.Has()
	return !has || err != nil
}

func (p *managedPublisher) Publish(event beat.Event, cursor interface{}) error {
	op, err := p.res.UpdateOp(cursor)
	if err != nil {
		return err
	}

	event.Private = op
	p.client.Publish(event)
	return p.canceler.Err()
}

func (c *managedCursor) Unpack(to interface{}) error   { return c.res.Read(to) }
func (c *managedCursor) Migrate(val interface{}) error { return c.res.Replace(val) }
