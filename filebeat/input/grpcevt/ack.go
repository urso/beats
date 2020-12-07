package grpcevt

import (
	"sync"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common/acker"
)

type batchACKHandler struct {
	closeOnce sync.Once

	breaker chan struct{} // short circuite ACK handling so we do not block on ch
	ch      chan int

	recording bool
	n         int
}

func newPipelineACKer() beat.ACKer {
	return acker.ConnectionOnly(acker.EventPrivateReporter(func(n int, priv []interface{}) {
		for _, p := range priv {
			if handler, ok := p.(*batchACKHandler); ok {
				handler.startRecording()
				handler.add()
			}
		}

		for _, p := range priv {
			if handler, ok := p.(*batchACKHandler); ok {
				handler.ack()
			}
		}

	}))
}

func newBatchACKHandler() *batchACKHandler {
	return &batchACKHandler{
		ch:      make(chan int, 1),
		breaker: make(chan struct{}),
	}
}

func (b *batchACKHandler) cancel() {
	b.closeOnce.Do(func() {
		close(b.breaker)
	})
}

func (b *batchACKHandler) startRecording() {
	b.recording = true
}

func (b *batchACKHandler) add() {
	if b.recording {
		b.n++
	}
}

func (b *batchACKHandler) ack() {
	if b.recording {
		n := b.n
		b.n = 0
		b.recording = false

		select {
		case b.ch <- n:
		case <-b.breaker:
		}
	}
}
