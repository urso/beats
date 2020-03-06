package urlcollect

import (
	v2 "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/filebeat/input/v2/statestore"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/registry"
)

// InputManager controls the execution of active inputs.
type InputManager struct {
	log      *logp.Logger
	registry *registry.Registry

	storeConnector *statestore.Connector
}

type Cursor interface {
	// IsNew returns true if the cursor is not yet known by the registry.
	IsNew() bool

	// Unpack reads the cursor into a custom data structure
	Unpack(to interface{}) error

	// Migrate updates the cursot contents. Migrate should only be used to update
	// the internal cursor format/schema. The Migrate operation will completely delete the old entry and replace it with a new one.
	// Do not use Migrate to store the recent cursor state in the registry.
	Migrate(interface{}) error
}

type managedCursor struct {
	res statestore.Resource
}

func NewInputManager(log *logp.Logger, registry *registry.Registry) *InputManager {
	required(registry != nil, "no registry")
	required(log != nil, "no logger")

	log = log.Named("urlcollect")
	return &InputManager{
		log:            log,
		registry:       registry,
		storeConnector: statestore.NewConnector(log, registry),
	}
}

func (m InputManager) withCursor(cancel v2.Canceler, key string, fn func(c *managedCursor)) {
	panic("TODO")
}

func (c *managedCursor) IsNew() bool {
	has, err := !c.res.Has()
	return !has || err != nil
}

func (c *managedCursor) Unpack(to interface{}) error   { return c.res.Read(to) }
func (c *managedCursor) Migrate(val interface{}) error { return c.res.Replace(val) }
