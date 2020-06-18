package cursor

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/statestore"
	"github.com/elastic/beats/v7/libbeat/statestore/storetest"
)

type testStateStore struct {
	Store    *statestore.Store
	GCPeriod time.Duration
}

func TestStore_OpenClose(t *testing.T) {
	t.Run("releasing store closes", func(t *testing.T) {
		var closed bool
		cleanup := closeStoreWith(func(s *store) {
			closed = true
			s.close()
		})
		defer cleanup()

		store := testOpenStore(t, "test", nil)
		store.Release()

		require.True(t, closed)
	})

	t.Run("fail if persistent store can not be accessed", func(t *testing.T) {
		_, err := openStore(logp.NewLogger("test"), testStateStore{}, "test")
		require.Error(t, err)
	})

	t.Run("load from empty", func(t *testing.T) {
		store := testOpenStore(t, "test", createSampleStore(t, nil))
		defer store.Release()
		require.Equal(t, 0, len(storeMemorySnapshot(store)))
	})

	t.Run("already available state is loaded", func(t *testing.T) {
		states := map[string]state{
			"test::key0": state{Cursor: "1"},
			"test::key1": state{Cursor: "2"},
		}

		store := testOpenStore(t, "test", createSampleStore(t, states))
		defer store.Release()

		require.Equal(t, states, storeMemorySnapshot(store))
	})

	t.Run("ignore entries with wrong index on open", func(t *testing.T) {
		states := map[string]state{
			"test::key0": state{Cursor: "1"},
			"other::key": state{Cursor: "2"},
		}

		store := testOpenStore(t, "test", createSampleStore(t, states))
		defer store.Release()

		want := map[string]state{
			"test::key0": state{Cursor: "1"},
		}
		require.Equal(t, want, storeMemorySnapshot(store))
	})
}

func TestStore_Get(t *testing.T) {
	t.Run("find existing resource", func(t *testing.T) {
		cursorState := state{Cursor: "1"}
		store := testOpenStore(t, "test", createSampleStore(t, map[string]state{
			"test::key0": cursorState,
		}))
		defer store.Release()

		res := store.Get("test::key0")
		require.NotNil(t, res)
		defer res.Release()

		// check in memory state matches matches original persistent state
		require.Equal(t, cursorState, res.stateSnapshot())
		// check assumed in-sync state matches matches original persistent state
		require.Equal(t, cursorState, res.inSyncStateSnapshot())
	})

	t.Run("access unknown resource", func(t *testing.T) {
		store := testOpenStore(t, "test", createSampleStore(t, nil))
		defer store.Release()

		res := store.Get("test::key")
		require.NotNil(t, res)
		defer res.Release()

		// new resource has empty state
		require.Equal(t, state{}, res.stateSnapshot())
	})

	t.Run("same resource is returned", func(t *testing.T) {
		store := testOpenStore(t, "test", createSampleStore(t, nil))
		defer store.Release()

		res1 := store.Get("test::key")
		require.NotNil(t, res1)
		defer res1.Release()

		res2 := store.Get("test::key")
		require.NotNil(t, res2)
		defer res2.Release()

		assert.Equal(t, res1, res2)
	})
}

func TestStore_UpdateTTL(t *testing.T) {
}

func closeStoreWith(fn func(s *store)) func() {
	old := closeStore
	closeStore = fn
	return func() {
		closeStore = old
	}
}

func testOpenStore(t *testing.T, prefix string, persistentStore StateStore) *store {
	if persistentStore == nil {
		persistentStore = createSampleStore(t, nil)
	}

	store, err := openStore(logp.NewLogger("test"), persistentStore, prefix)
	if err != nil {
		t.Fatalf("failed to open the store")
	}
	return store
}

func createSampleStore(t *testing.T, data map[string]state) testStateStore {
	storeReg := statestore.NewRegistry(storetest.NewMemoryStoreBackend())
	store, err := storeReg.Get("test")
	if err != nil {
		t.Fatalf("Failed to access store: %v", err)
	}

	for k, v := range data {
		if err := store.Set(k, v); err != nil {
			t.Fatalf("Error when populating the sample store: %v", err)
		}
	}

	return testStateStore{
		Store: store,
	}
}

func (ts testStateStore) WithGCPeriod(d time.Duration) testStateStore { ts.GCPeriod = d; return ts }
func (ts testStateStore) CleanupInterval() time.Duration              { return ts.GCPeriod }
func (ts testStateStore) Access() (*statestore.Store, error) {
	if ts.Store == nil {
		return nil, errors.New("no store configured")
	}
	return ts.Store, nil
}

func (ts testStateStore) snapshot() map[string]state {
	states := map[string]state{}
	err := ts.Store.Each(func(key string, dec statestore.ValueDecoder) (bool, error) {
		var st state
		if err := dec.Decode(&st); err != nil {
			return false, err
		}
		states[key] = st
		return true, nil
	})

	if err != nil {
		panic("unexpected decode error from persistent test store")
	}
	return states
}

func storeMemorySnapshot(store *store) map[string]state {
	store.ephemeralStore.mu.Lock()
	defer store.ephemeralStore.mu.Unlock()

	states := map[string]state{}
	for k, res := range store.ephemeralStore.table {
		states[k] = res.stateSnapshot()
	}
	return states
}

func storeInSyncSnapshot(store *store) map[string]state {
	store.ephemeralStore.mu.Lock()
	defer store.ephemeralStore.mu.Unlock()

	states := map[string]state{}
	for k, res := range store.ephemeralStore.table {
		states[k] = res.inSyncStateSnapshot()
	}
	return states
}
