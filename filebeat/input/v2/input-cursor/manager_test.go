package cursor

import (
	"context"
	"errors"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	input "github.com/elastic/beats/v7/filebeat/input/v2"
	v2 "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/tests/resources"
	"github.com/elastic/go-concert/unison"
	"github.com/stretchr/testify/require"
)

type fakeTestInput struct {
	OnTest func(Source, input.TestContext) error
	OnRun  func(input.Context, Source, Cursor, Publisher) error
}

type stringSource string

func TestManager_Init(t *testing.T) {
	// Integration style tests for the InputManager and the state garbage collector

	t.Run("stopping the taskgroup kills internal go-routines", func(t *testing.T) {
		numRoutines := runtime.NumGoroutine()

		var grp unison.TaskGroup
		store := createSampleStore(t, nil)
		manager := &InputManager{
			Logger:              logp.NewLogger("test"),
			StateStore:          store,
			Type:                "test",
			DefaultCleanTimeout: 10 * time.Millisecond,
		}

		err := manager.Init(&grp, v2.ModeRun)
		require.NoError(t, err)

		time.Sleep(200 * time.Millisecond)
		grp.Stop()

		// wait for all go-routines to be gone

		for numRoutines < runtime.NumGoroutine() {
			time.Sleep(1 * time.Millisecond)
		}
	})

	t.Run("collect old entries after startup", func(t *testing.T) {
		store := createSampleStore(t, map[string]state{
			"test::key": {
				TTL:     1 * time.Millisecond,
				Updated: time.Now().Add(-24 * time.Hour),
			},
		})
		store.GCPeriod = 10 * time.Millisecond

		var grp unison.TaskGroup
		defer grp.Stop()
		manager := &InputManager{
			Logger:              logp.NewLogger("test"),
			StateStore:          store,
			Type:                "test",
			DefaultCleanTimeout: 10 * time.Millisecond,
		}

		err := manager.Init(&grp, v2.ModeRun)
		require.NoError(t, err)

		for len(store.snapshot()) > 0 {
			time.Sleep(1 * time.Millisecond)
		}
	})
}

func TestManager_Create(t *testing.T) {
	t.Run("fail if no source is configured", func(t *testing.T) {
		manager := &InputManager{
			Logger:     logp.NewLogger("test"),
			StateStore: createSampleStore(t, nil),
			Configure: func(cfg *common.Config) ([]Source, Input, error) {
				return nil, &fakeTestInput{}, nil
			},
		}

		_, err := manager.Create(common.NewConfig())
		require.Error(t, err)
	})

	t.Run("fail if config error", func(t *testing.T) {
		manager := &InputManager{
			Logger:     logp.NewLogger("test"),
			StateStore: createSampleStore(t, nil),
			Configure: func(cfg *common.Config) ([]Source, Input, error) {
				return nil, nil, errors.New("oops")
			},
		}

		_, err := manager.Create(common.NewConfig())
		require.Error(t, err)
	})

	t.Run("fail if no input runner is returned", func(t *testing.T) {
		manager := &InputManager{
			Logger:     logp.NewLogger("test"),
			StateStore: createSampleStore(t, nil),
			Configure: func(cfg *common.Config) ([]Source, Input, error) {
				return []Source{stringSource("test")}, nil, nil
			},
		}

		_, err := manager.Create(common.NewConfig())
		require.Error(t, err)
	})

	t.Run("configure ok", func(t *testing.T) {
		manager := &InputManager{
			Logger:     logp.NewLogger("test"),
			StateStore: createSampleStore(t, nil),
			Configure: func(cfg *common.Config) ([]Source, Input, error) {
				return []Source{stringSource("test")}, &fakeTestInput{}, nil
			},
		}

		_, err := manager.Create(common.NewConfig())
		require.NoError(t, err)
	})

	t.Run("configuring inputs with overlapping sources is allowed", func(t *testing.T) {
		manager := &InputManager{
			Logger:     logp.NewLogger("test"),
			StateStore: createSampleStore(t, nil),
			Configure: func(cfg *common.Config) ([]Source, Input, error) {
				config := struct {
					Sources []string
				}{}
				err := cfg.Unpack(&config)

				arr := make([]Source, len(config.Sources))
				for i, name := range config.Sources {
					arr[i] = stringSource(name)
				}

				return arr, &fakeTestInput{}, err
			},
		}

		_, err := manager.Create(common.MustNewConfigFrom(map[string]interface{}{
			"sources": []string{"a"},
		}))
		require.NoError(t, err)

		_, err = manager.Create(common.MustNewConfigFrom(map[string]interface{}{
			"sources": []string{"a"},
		}))
		require.NoError(t, err)
	})
}

func TestManager_InputsTest(t *testing.T) {
	var mu sync.Mutex
	var seen []string

	sources := []Source{stringSource("source1"), stringSource("source2")}

	t.Run("test is run for each source", func(t *testing.T) {
		manager := &InputManager{
			Logger:     logp.NewLogger("test"),
			StateStore: createSampleStore(t, nil),
			Configure: func(cfg *common.Config) ([]Source, Input, error) {
				return sources, &fakeTestInput{
					OnTest: func(source Source, _ v2.TestContext) error {
						mu.Lock()
						defer mu.Unlock()
						seen = append(seen, source.Name())
						return nil
					},
				}, nil
			},
		}

		inp, err := manager.Create(common.NewConfig())
		require.NoError(t, err)

		err = inp.Test(input.TestContext{})
		require.NoError(t, err)

		sort.Strings(seen)
		require.Equal(t, []string{"source1", "source2"}, seen)
	})

	t.Run("cancel gets distributed to all source tests", func(t *testing.T) {
		defer resources.NewGoroutinesChecker().Check(t)

		sources := []Source{stringSource("source1"), stringSource("source2")}

		manager := &InputManager{
			Logger:     logp.NewLogger("test"),
			StateStore: createSampleStore(t, nil),
			Configure: func(cfg *common.Config) ([]Source, Input, error) {
				return sources, &fakeTestInput{
					OnTest: func(_ Source, ctx v2.TestContext) error {
						<-ctx.Cancelation.Done()
						return nil
					},
				}, nil
			},
		}

		inp, err := manager.Create(common.NewConfig())
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.TODO())

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			err = inp.Test(input.TestContext{Cancelation: ctx})
		}()

		cancel()
		wg.Wait()
		require.NoError(t, err)
	})

	t.Run("fail if test for one source fails", func(t *testing.T) {
		defer resources.NewGoroutinesChecker().Check(t)

		failing := Source(stringSource("source1"))
		sources := []Source{failing, stringSource("source2")}

		manager := &InputManager{
			Logger:     logp.NewLogger("test"),
			StateStore: createSampleStore(t, nil),
			Configure: func(cfg *common.Config) ([]Source, Input, error) {
				return sources, &fakeTestInput{
					OnTest: func(source Source, _ v2.TestContext) error {
						if source == failing {
							t.Log("return error")
							return errors.New("oops")
						}
						t.Log("return ok")
						return nil
					},
				}, nil
			},
		}

		inp, err := manager.Create(common.NewConfig())
		require.NoError(t, err)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			err = inp.Test(input.TestContext{})
			t.Logf("Test returned: %v", err)
		}()

		wg.Wait()
		require.Error(t, err)
	})
}

func TestManager_InputsRun(t *testing.T) {
	t.Run("input returned with error", func(t *testing.T) {
	})

	t.Run("inputs are executed concurrently", func(t *testing.T) {
	})

	t.Run("shutdown on signal", func(t *testing.T) {
	})

	t.Run("event ACK triggers execution of update operations", func(t *testing.T) {
	})
}

func TestLockResource(t *testing.T) {
	t.Run("can lock unused resource", func(t *testing.T) {
		store := testOpenStore(t, "test", createSampleStore(t, nil))
		defer store.Release()

		res := store.Get("test::key")
		err := lockResource(logp.NewLogger("test"), res, context.TODO())
		require.NoError(t, err)
	})

	t.Run("fail to lock resource in use when context is cancelled", func(t *testing.T) {
		log := logp.NewLogger("test")

		store := testOpenStore(t, "test", createSampleStore(t, nil))
		defer store.Release()

		resUsed := store.Get("test::key")
		err := lockResource(log, resUsed, context.TODO())
		require.NoError(t, err)

		// fail to lock resource in use
		ctx, cancel := context.WithCancel(context.TODO())
		cancel()
		resFail := store.Get("test::key")
		err = lockResource(log, resFail, ctx)
		require.Error(t, err)
		resFail.Release()

		// unlock and release resource in use -> it should be marked finished now
		releaseResource(resUsed)
		require.True(t, resUsed.Finished())
	})

	t.Run("succeed to lock resource after it has been released", func(t *testing.T) {
		log := logp.NewLogger("test")

		store := testOpenStore(t, "test", createSampleStore(t, nil))
		defer store.Release()

		resUsed := store.Get("test::key")
		err := lockResource(log, resUsed, context.TODO())
		require.NoError(t, err)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			resOther := store.Get("test::key")
			err := lockResource(log, resOther, context.TODO())
			if err == nil {
				releaseResource(resOther)
			}
		}()

		go func() {
			time.Sleep(100 * time.Millisecond)
			releaseResource(resUsed)
		}()

		wg.Wait() // <- block forever if waiting go-routine can not acquire lock
	})
}

func (s stringSource) Name() string { return string(s) }

func (f *fakeTestInput) Name() string { return "test" }

func (f *fakeTestInput) Test(source Source, ctx input.TestContext) error {
	if f.OnTest != nil {
		return f.OnTest(source, ctx)
	}
	return nil
}

func (f *fakeTestInput) Run(ctx input.Context, source Source, cursor Cursor, pub Publisher) error {
	if f.OnRun != nil {
		return f.OnRun(ctx, source, cursor, pub)
	}
	return nil
}
