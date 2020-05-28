package badgerdb

import (
	"testing"

	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/storetest/backend"
	"github.com/elastic/beats/v7/libbeat/storetest/backend/storetests"
)

func init() {
	logp.DevelopmentSetup()
}

func TestCompliance(t *testing.T) {
	storetests.TestBackendCompliance(t, func(testPath string) (backend.Registry, error) {
		logger := logp.NewLogger("test")
		return New(logger, testPath), nil
	})
}
