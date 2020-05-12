package kafka

import (
	input "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/go-concert/unison"
)

type inputManager struct {
	Configure func(*common.Config) (input.Input, error)
}

// Init is required to fullfil the input.InputManager interface.
// For the kafka input no special initialization is required.
func (*inputManager) Init(grp unison.Group, m input.Mode) error { return nil }

// Creates builds a new Input instance from the given configuation, or returns
// an error if the configuation is invalid.
func (manager *inputManager) Create(cfg *common.Config) (input.Input, error) {
	return manager.Configure(cfg)
}
