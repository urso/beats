package urlcollect

import "sync"

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
