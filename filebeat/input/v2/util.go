package v2

import (
	"sync"

	"github.com/elastic/beats/v7/libbeat/common"
)

func getTypeName(cfg *common.Config, field string) (string, error) {
	if field == "" {
		field = "type"
	}
	return cfg.String(field, -1)
}

func typeFieldName(in string) string {
	if in == "" {
		return "type"
	}
	return in
}

type sharedErr struct {
	mu   sync.Mutex
	errs []error
}

type group struct {
	runErrs sharedErr
	wg      sync.WaitGroup
}

func (s *sharedErr) Add(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errs = append(s.errs, err)
}

func (s *sharedErr) Errors() []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.errs
}

func (g *group) Go(fn func() error) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		g.runErrs.Add(fn())
	}()
}

func (g *group) Wait() []error {
	g.wg.Wait()
	return g.runErrs.Errors()
}
