package v2

import (
	"context"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
)

// Input is a configured input object that can be used to probe or start
// a actual data processing.
//
// TODO: Discuss struct vs. interface
//
//
type Input struct {
	Name string
	Run  func(Context, beat.PipelineConnector) error
	Test func(TestContext) error
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

type cancelChan <-chan struct{}

// ChanCanceler wraps a channerl into a Canceler.
func ChanCanceler(ch <-chan struct{}) Canceler { return cancelChan(ch) }
func (ch cancelChan) Done() <-chan struct{}    { return ch }
func (ch cancelChan) Err() error {
	select {
	case <-ch:
		return context.Canceled
	default:
		return nil
	}
}
