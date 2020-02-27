package v2

// OptStatusObserver can create a StatusObserver based on function pointers
// only. Function pointers can be nil.
type OptStatusObserver struct {
	Starting    func()
	Stopped     func()
	Failed      func(error)
	Initialized func()
	Active      func()
	Failing     func(error)
	Stopping    func()
}

type optObserver struct{ fns OptStatusObserver }

// StatusObserver is used to report a standardized set of state change events
// of an input.
type StatusObserver interface {
	RunnerObserver

	// Starting indicates that the input is about to be configured and started.
	Starting()

	// Stopped reports that the input has finished the shutdown and cleanup.
	Stopped()

	// Failed indicates that the input has been stopped due to a fatal error.
	Failed(err error)
}

// RunnerObserver reports the current state of an active input instance.
type RunnerObserver interface {
	// Initialized reports that required resources are initialized, but the
	// Input is not collecting events yet.
	Initialized()

	// Active reports that the input is about to start collecting events.
	Active()

	// Failing reports that the input is experiencing temporary errors. The input
	// does not quit yet, but will attempt to retry.
	Failing(err error)

	// Stopping reports that the input is about to stop and clean up resources.
	Stopping()
}

// Create builds a StatusObserver based on the given configuration.
func (oso OptStatusObserver) Create() StatusObserver {
	return &optObserver{oso}
}

func (o *optObserver) Starting()         { callIf(o.fns.Starting) }
func (o *optObserver) Stopped()          { callIf(o.fns.Stopped) }
func (o *optObserver) Failed(err error)  { callErrIf(o.fns.Failed, err) }
func (o *optObserver) Initialized()      { callIf(o.fns.Initialized) }
func (o *optObserver) Active()           { callIf(o.fns.Active) }
func (o *optObserver) Failing(err error) { callErrIf(o.fns.Failing, err) }
func (o *optObserver) Stopping()         { callIf(o.fns.Stopping) }

func callIf(f func()) {
	if f != nil {
		f()
	}
}

func callErrIf(f func(error), err error) {
	if f != nil {
		f(err)
	}
}
