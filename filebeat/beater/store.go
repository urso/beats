package beater

import (
	"time"

	"github.com/elastic/beats/v7/filebeat/config"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/paths"
	"github.com/elastic/beats/v7/libbeat/statestore"
	"github.com/elastic/beats/v7/libbeat/statestore/backend/memlog"
)

type filebeatStore struct {
	registry      *statestore.Registry
	storeName     string
	cleanInterval time.Duration
}

func openStateStore(info beat.Info, cfg config.Registry) (*filebeatStore, error) {
	memlog, err := memlog.New(memlog.Settings{
		Root:     paths.Resolve(paths.Data, cfg.Path),
		FileMode: cfg.Permissions,
	})
	if err != nil {
		return nil, err
	}

	return &filebeatStore{
		registry:      statestore.NewRegistry(memlog),
		storeName:     info.Beat,
		cleanInterval: cfg.CleanInterval,
	}, nil
}

func (s *filebeatStore) Close() {
	s.registry.Close()
}

func (s *filebeatStore) Access() (*statestore.Store, error) {
	return s.registry.Get(s.storeName)
}

func (s *filebeatStore) CleanupInterval() time.Duration {
	return s.cleanInterval
}
