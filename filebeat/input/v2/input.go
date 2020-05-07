package v2

import (
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"

	"github.com/elastic/go-concert/unison"
)

// InputManager creates and maintains actions and background processes for an
// input type.
type InputManager interface {
	// Init signals to InputManager to initialize internal resources.
	Init(grp unison.Group, m Mode) error

	// Creates builds a new Input instance from the given configuation, or returns
	// an error if the configuation is invalid.
	// The generated must not collect any data yet. The Beat will use the Test/Run
	// methods of the input.
	Create(*common.Config) (Input, error)
}

// Mode tells the InputManager in which mode it is initialized.
type Mode uint8

//go:generate stringer -type Mode -trimprefix Mode
const (
	ModeRun Mode = iota
	ModeTest
	ModeOther
)

// Input is a configured input object that can be used to probe or start
// the actual data processing.
type Input interface {
	// TODO: check if/how we can remove this method. Currently it is required for
	// compatibility reasons with existing interfaces in libbeat, autodiscovery
	// and filebeat.
	Name() string

	// Test checks the configuaration and runs addition checks if the Input can be
	// initialized from the configuration (e.g. check if host/port or files are
	// accessible).
	Test(TestContext) error

	// Run executes the data collection loop. Run must return an error only if the
	// error can not be recovered.
	Run(Context, beat.PipelineConnector) error
}

// Context provides the Input Run function with common environmental
// information and services.
type Context struct {
	ID          string
	Agent       beat.Info
	Logger      *logp.Logger
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
