package v2

import (
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
)

// InputManager creates and maintains actions and background processes for an
// input type.
type InputManager interface {
	// CreateServices creates an input types background service if applicable.
	// If the input type does not require any kind of background services, nil
	// shall be returned.
	// The service will be run by the Beat, even if no Input is active. This
	// allows the service to run maintenance tasks for the input type even if no
	// input is currently active (for example remove entries from the registry).
	//
	// Note: Beats will always instantiate all background services upon startup.
	//       Background services are shut down only after all inputs have been
	//       stopped.
	CreateService(fullInputName string) (BackgroundService, error)

	// Creates builds a new Input instance from the given configuation, or returns
	// an error if the configuation is invalid.
	// The generated must not collect any data yet. The Beat will use the Test/Run
	// methods of the input.
	Create(*common.Config) (Input, error)
}

// Input is a configured input object that can be used to probe or start
// the actual data processing.
type Input interface {
	Test(TestContext) error
	Run(Context, beat.PipelineConnector) error
}

// Context provides the Input Run function with common environmental
// information and services.
type Context struct {
	ID          string
	Agent       beat.Info
	Logger      *logp.Logger
	Status      RunnerObserver
	Metadata    *common.MapStrPointer // XXX: from Autodiscovery.
	Cancelation Canceler
}

// TestContext provides the Input Test function with common environmental
// information and services.
type TestContext struct {
	Agent       beat.Info
	Logger      *logp.Logger
	Cancelation Canceler
}

// Canceler is used to provide shutdown handling to the Context.
type Canceler interface {
	Done() <-chan struct{}
	Err() error
}
