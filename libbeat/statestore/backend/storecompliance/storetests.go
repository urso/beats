package storecompliance

import (
	"flag"
	"testing"

	"github.com/elastic/beats/v7/libbeat/statestore/backend"
	"github.com/stretchr/testify/assert"
)

type BackendFactory func(testPath string) (backend.Registry, error)

var defaultTempDir string
var keepTmpDir bool

func init() {
	flag.StringVar(&defaultTempDir, "dir", "", "Temporary directory for use by the test")
	flag.BoolVar(&keepTmpDir, "keep", false, "Keep temporary test directories")
}

func TestBackendCompliance(t *testing.T, factory BackendFactory) {
	t.Run("init and close registry", WithPath(factory, func(t *testing.T, reg *Registry) {
	}))

	t.Run("open stores", WithPath(factory, func(t *testing.T, reg *Registry) {
		store := reg.MustAccess("test1")
		defer store.Close()

		store2 := reg.MustAccess("test2")
		defer store2.Close()
	}))

	t.Run("set-get", withBackend(factory, testSetGet))
	t.Run("remove", withBackend(factory, testRemove))
	t.Run("iteration", withBackend(factory, testIteration))
}

func testSetGet(t *testing.T, factory BackendFactory) {
	t.Run("unknown key", WithStore(factory, func(t *testing.T, store *Store) {
		has := store.MustHas("key")
		assert.False(t, has)
	}))

	runWithBools(t, "reopen", func(t *testing.T, reopen bool) {
		t.Run("has key after set", WithStore(factory, func(t *testing.T, store *Store) {
			type entry struct{ A int }
			store.MustSet("key", entry{A: 1})

			store.ReopenIf(reopen)

			has := store.MustHas("key")
			assert.True(t, has)
		}))

		t.Run("set and get one entry only", WithStore(factory, func(t *testing.T, store *Store) {
			type entry struct{ A int }
			key := "key"
			value := entry{A: 1}

			store.MustSet(key, value)
			store.ReopenIf(reopen)

			var actual entry
			store.MustGet(key, &actual)
			assert.Equal(t, value, actual)
		}))
	})
}

func testRemove(t *testing.T, factory BackendFactory) {
	t.Run("no error when removing unknown key", WithStore(factory, func(t *testing.T, store *Store) {
		store.MustRemove("key")
	}))

	runWithBools(t, "reopen", func(t *testing.T, reopen bool) {
		t.Run("remove key", WithStore(factory, func(t *testing.T, store *Store) {
			type entry struct{ A int }
			key := "key"
			store.MustSet(key, entry{A: 1})
			store.ReopenIf(reopen)
			store.MustRemove(key)
			store.ReopenIf(reopen)
			has := store.MustHas(key)
			assert.False(t, has)
		}))
	})
}

func testIteration(t *testing.T, factory BackendFactory) {
}
