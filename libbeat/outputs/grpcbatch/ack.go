package grpcbatch

import (
	"github.com/elastic/beats/v7/libbeat/common/atomic"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/publisher"
)

type batchRef struct {
	logger *logp.Logger
	stats  outputs.Observer
	count  atomic.Uint32
	batch  publisher.Batch
	err    error
}

func (r *batchRef) dec() {
	i := r.count.Dec()
	if i > 0 {
		return
	}

	err := r.err
	if err == nil {
		r.batch.ACK()
		r.stats.Acked(len(r.batch.Events()))
		return
	}

	r.logger.Errorf("Failed to publish %v events: %v", len(r.batch.Events()), err)
	r.stats.Failed(len(r.batch.Events()))
	r.batch.Retry()
}

func (r *batchRef) fail(err error) {
	if r.err == nil {
		r.err = err
	}

	r.dec()
}
