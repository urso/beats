package pipeline

import (
	"sync"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common/atomic"
)

// ACKer can be registered with a Client when connecting to the pipeline.
// The ACKer will be informed when events are added or dropped by the processors,
// and when an event has been ACKed by the outputs.
type ACKer = beat.ACKer

func NilACKer() ACKer {
	return nilACKer{}
}

type nilACKer struct{}

func (nilACKer) AddEvent(event beat.Event, published bool) {}
func (nilACKer) ACKEvents(n int)                           {}
func (nilACKer) Close()                                    {}

// CountACKer reports the number of ACKed events as has been reported by the outputs or queue.
// The ACKer does not keep track of dropped events. Events after the client has
// been closed will still be reported.
func CountACKer(fn func(int)) ACKer {
	return countACKer(fn)
}

type countACKer func(int)

func (countACKer) AddEvent(_ beat.Event, _ bool) {}
func (fn countACKer) ACKEvents(n int)            { fn(n) }
func (countACKer) Close()                        {}

// TrackingCountACKer keeps track of published and dropped events. It reports
// the number of acked events from the queue in the 'acked' argument and the
// total number of events published via the Client in the 'total' argument.
// The TrackingCountACKer keeps track of the order of events being send and events being acked.
// If N events have been acked by the output, then `total` will include all events dropped in between
// the last forwarded N events and the 'tail' of dropped events. For example (X = send, D = dropped):
//
//  index: 0  1  2  3  4  5  6  7  8  9  10  11
//  event: X  X  D  D  X  D  D  X  D  X   X   X
//
// If the output ACKs 3 events, then all events from index 0 to 6 will be reported because:
// - the drop sequence for events 2 and 3 is inbetween the number of forwarded and ACKed events
// - events 5-6 have been dropped as well, but event 7 is not ACKed yet
//
// If there is no event currently tracked by this ACKer and the next event is dropped by the processors,
// then `fn` will be called immediately with acked=0 and total=1.
func TrackingCountACKer(fn func(acked, total int)) ACKer {
	a := &trackingACKer{fn: fn}
	init := &gapInfo{}
	a.lst.head = init
	a.lst.tail = init
	return a
}

type trackingACKer struct {
	fn     func(acked, total int)
	events atomic.Uint32
	lst    gapList
}

type gapList struct {
	sync.Mutex
	head, tail *gapInfo
}

type gapInfo struct {
	sync.Mutex
	next          *gapInfo
	send, dropped int
}

func (a *trackingACKer) AddEvent(_ beat.Event, published bool) {
	a.events.Inc()
	if published {
		a.addPublishedEvent()
	} else {
		a.addDropEvent()
	}
}

// addPublishedEvent increments the 'send' counter in the current gapInfo
// element in the tail of the list. If events have been dropped, we append a
// new empty gapInfo element.
func (a *trackingACKer) addPublishedEvent() {
	a.lst.Lock()

	current := a.lst.tail
	current.Lock()
	if current.dropped > 0 {
		tmp := &gapInfo{}
		tmp.Lock()

		a.lst.tail.next = tmp
		a.lst.tail = tmp
		current.Unlock()
		current = tmp
	}
	a.lst.Unlock()

	current.send++
	current.Unlock()
}

// addDropEvent increments the 'dropped' counter in the gapInfo element in the
// tail of the list.  The callback will be run with total=1 and acked=0 if the
// acker state is empty and no events have been send yet.
func (a *trackingACKer) addDropEvent() {
	a.lst.Lock()

	current := a.lst.tail
	current.Lock()

	if current.send == 0 && current.next == nil {
		// send can only be 0 if no no events/gaps present yet
		if a.lst.head != a.lst.tail {
			panic("gap list expected to be empty")
		}

		a.fn(0, 1)
		a.lst.Unlock()
		current.Unlock()

		a.events.Dec()
		return
	}

	a.lst.Unlock()
	current.dropped++
	current.Unlock()
}

func (a *trackingACKer) ACKEvents(n int) {
	var (
		total    = 0
		acked    = n
		emptyLst bool
	)

	for n > 0 {
		if emptyLst {
			panic("too many events acked")
		}

		a.lst.Lock()
		current := a.lst.head
		current.Lock()

		// advance list if we detect that the current head will be completely consumed
		// by this ACK event.
		if n >= current.send {
			next := current.next
			emptyLst = next == nil
			if !emptyLst {
				// advance list all event in current entry have been send and list as
				// more then 1 gapInfo entry. If only 1 entry is present, list item will be
				// reset and reused
				a.lst.head = next
			}
		}
		// hand over lock list-entry, so ACK handler and producer can operate
		// on potentially different list ends
		a.lst.Unlock()

		if n < current.send {
			current.send -= n
			total += n
			n = 0
		} else {
			total += current.send + current.dropped
			n -= current.send
			current.dropped = 0
			current.send = 0
		}
		current.Unlock()
	}

	a.events.Sub(uint32(total))
	a.fn(acked, total)
}

func (a *trackingACKer) Close() {}

// EventPrivateFieldsACKer reports all private fields from all events that have
// been published or removed.
//
// The EventPrivateFieldsACKer keeps track of the order of events being send
// and events being acked.  If N events have been acked by the output, then
// `total` will include all events dropped in between the last forwarded N
// events and the 'tail' of dropped events. For example (X = send, D =
// dropped):
//
//  index: 0  1  2  3  4  5  6  7  8  9  10  11
//  event: X  X  D  D  X  D  D  X  D  X   X   X
//
// If the output ACKs 3 events, then all events from index 0 to 6 will be reported because:
// - the drop sequence for events 2 and 3 is inbetween the number of forwarded and ACKed events
// - events 5-6 have been dropped as well, but event 7 is not ACKed yet
func EventPrivateFieldsACKer(fn func(acked int, data []interface{})) ACKer {
	a := &eventDataACKer{fn: fn}
	a.ACKer = TrackingCountACKer(a.onACK)
	return a
}

type eventDataACKer struct {
	ACKer
	mu   sync.Mutex
	data []interface{}
	fn   func(acked int, data []interface{})
}

func (a *eventDataACKer) AddEvent(event beat.Event, published bool) {
	a.mu.Lock()
	a.data = append(a.data, event.Private)
	a.mu.Unlock()
	a.ACKer.AddEvent(event, published)
}

func (a *eventDataACKer) onACK(acked, total int) {
	if total == 0 {
		return
	}

	a.mu.Lock()
	data := a.data[:total]
	a.data = a.data[total:]
	a.mu.Unlock()

	if len(data) > 0 {
		a.fn(acked, data)
	}
}

// LastEventPrivateFieldsACKer reports only the 'latest' published and acked
// event if a batch of events have been ACKed.
func LastEventPrivateFieldsACKer(fn func(acked int, data interface{})) ACKer {
	return EventPrivateFieldsACKer(func(acked int, data []interface{}) {
		fn(acked, data[len(data)-1])
	})
}

// CombineACKers forwards events to a list of ackers.
func CombineACKers(as ...ACKer) ACKer {
	return ackerList(as)
}

type ackerList []ACKer

func (l ackerList) AddEvent(event beat.Event, published bool) {
	for _, a := range l {
		a.AddEvent(event, published)
	}
}

func (l ackerList) ACKEvents(n int) {
	for _, a := range l {
		a.ACKEvents(n)
	}
}

func (l ackerList) Close() {
	for _, a := range l {
		a.Close()
	}
}

// ConnectionOnly ensures that the given ACKer is only used for as long as the
// pipeline Client is active.  Once the Client is closed, the ACKer will drop
// it's internal state and no more ACK events will be processed.
func ConnectionOnly(a ACKer) ACKer {
	return &clientOnlyACKer{acker: a}
}

type clientOnlyACKer struct {
	mu    sync.Mutex
	acker ACKer
}

func (a *clientOnlyACKer) AddEvent(event beat.Event, published bool) {
	a.mu.Lock()
	sub := a.acker
	a.mu.Unlock()
	if sub != nil {
		sub.AddEvent(event, published)
	}
}

func (a *clientOnlyACKer) ACKEvents(n int) {
	a.mu.Lock()
	sub := a.acker
	a.mu.Unlock()
	if sub != nil {
		sub.ACKEvents(n)
	}
}

func (a *clientOnlyACKer) Close() {
	a.mu.Lock()
	sub := a.acker
	a.acker = nil // drop the internal ACKer on Close and allow the runtime to gc accumulated state.
	a.mu.Unlock()
	if sub != nil {
		sub.Close()
	}
}
