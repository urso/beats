package esbulk

import (
	"context"
	"sync"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common/acker"
)

type bulkACKHandler struct {
	// TODO: replace wg with semaphore or other type that can be cancelled on wait
	wg sync.WaitGroup
}

type eventACKHandler struct {
	bulk *bulkACKHandler
}

func newPipelineACKer() beat.ACKer {
	return acker.ConnectionOnly(acker.EventPrivateReporter(func(_ int, events []interface{}) {
		for _, event := range events {
			if handler, ok := event.(eventACKHandler); ok {
				handler.ack()
			}
		}
	}))
}

func newBulkACKHandler() *bulkACKHandler {
	return &bulkACKHandler{}
}

func (h *bulkACKHandler) wait(ctx context.Context) error {
	h.wg.Wait()
	return ctx.Err()
}

func (h *bulkACKHandler) addEvent() eventACKHandler {
	h.wg.Add(1)
	return eventACKHandler{h}
}

func (h *eventACKHandler) ack() {
	h.bulk.wg.Done()
}
