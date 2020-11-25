package lumberjack

import (
	"sync"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common/acker"
	"github.com/elastic/go-lumber/lj"
)

type batchACKHandler struct {
	mu      sync.Mutex
	pending int
	batch   *lj.Batch
}

func newPipelineACKer() beat.ACKer {
	return acker.ConnectionOnly(acker.LastEventPrivateReporter(func(n int, priv interface{}) {
		if handler, ok := priv.(*batchACKHandler); ok {
			handler.ack(n)
		}
	}))
}

func newBatchACKHandler(batch *lj.Batch) *batchACKHandler {
	return &batchACKHandler{
		pending: len(batch.Events),
		batch:   batch,
	}
}

func (h *batchACKHandler) ack(n int) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.pending -= n
	if h.pending <= 0 {
		h.batch.ACK()
		h.batch = nil
	}
}
