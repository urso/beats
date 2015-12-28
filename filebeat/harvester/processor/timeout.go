package processor

import (
	"errors"
	"time"
)

var (
	errTimeout = errors.New("timeout")
)

type timeoutProcessor struct {
	reader  LineProcessor
	timeout time.Duration
	signal  error

	running bool
	ch      chan lineMessage
}

func newTimeoutProcessor(in LineProcessor, signal error, timeout time.Duration) *timeoutProcessor {
	if signal == nil {
		signal = errTimeout
	}

	return &timeoutProcessor{
		reader:  in,
		signal:  signal,
		timeout: timeout,
		ch:      make(chan lineMessage, 1),
	}
}

func (tr *timeoutProcessor) Next() (Line, error) {
	if !tr.running {
		tr.running = true
		go func() {
			for {
				line, err := tr.reader.Next()
				tr.ch <- lineMessage{line, err}
				if err != nil {
					break
				}
			}
		}()
	}

	select {
	case msg := <-tr.ch:
		if msg.err != nil {
			tr.running = false
		}
		return msg.line, msg.err
	case <-time.After(tr.timeout):
		return Line{}, tr.signal
	}
}
