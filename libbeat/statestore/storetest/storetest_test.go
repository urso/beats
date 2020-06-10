package storetest

import (
	"testing"

	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/statestore/backend"
	"github.com/elastic/beats/v7/libbeat/statestore/backend/storecompliance"
	"github.com/stretchr/testify/assert"
)

func init() {
	logp.DevelopmentSetup()
}

func TestCompliance(t *testing.T) {
	storecompliance.TestBackendCompliance(t, func(testPath string) (backend.Registry, error) {
		return NewMemoryStoreBackend(), nil
	})
}

func TestStore_IsClosed(t *testing.T) {
	t.Run("false by default", func(t *testing.T) {
		store := &MapStore{}
		assert.False(t, store.IsClosed())
	})
	t.Run("true after close", func(t *testing.T) {
		store := &MapStore{}
		store.Close()
		assert.True(t, store.IsClosed())
	})
	t.Run("true after reopen", func(t *testing.T) {
		store := &MapStore{}
		store.Close()
		store.Reopen()
		assert.False(t, store.IsClosed())
	})
}
