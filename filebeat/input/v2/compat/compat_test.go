package compat

import (
	"sync"
	"testing"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v2 "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/filebeat/input/v2/internal/inputest"
)

func TestRunnerFactory_CheckConfig(t *testing.T) {
	t.Run("does not run or test configured input", func(t *testing.T) {
		log := logp.NewLogger("test")
		var countConfigure, countTest, countRun int

		// setup
		plugins := inputest.SinglePlugin("test", &inputest.MockInputManager{
			OnConfigure: func(_ *common.Config) (v2.Input, error) {
				countConfigure++
				return &inputest.MockInput{
					OnTest: func(_ v2.TestContext) error { countTest++; return nil },
					OnRun:  func(_ v2.Context, _ beat.PipelineConnector) error { countRun++; return nil },
				}, nil
			},
		})
		loader := inputest.MustNewTestLoader(t, plugins, "type", "test")
		factory := RunnerFactory(log, beat.Info{}, loader.Loader)

		// run
		err := factory.CheckConfig(common.NewConfig())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// validate: configured an input, but do not run test or run
		assert.Equal(t, 1, countConfigure)
		assert.Equal(t, 0, countTest)
		assert.Equal(t, 0, countRun)
	})

	t.Run("fail if input type is unknown to loader", func(t *testing.T) {
		log := logp.NewLogger("test")
		plugins := inputest.SinglePlugin("test", inputest.ConstInputManager(nil))
		loader := inputest.MustNewTestLoader(t, plugins, "type", "")
		factory := RunnerFactory(log, beat.Info{}, loader.Loader)

		// run
		err := factory.CheckConfig(common.MustNewConfigFrom(map[string]interface{}{
			"type": "unknown",
		}))
		assert.Error(t, err)
	})
}

func TestRunnerFactory_CreateAndRun(t *testing.T) {
	t.Run("runner can correctly start and stop inputs", func(t *testing.T) {
		log := logp.NewLogger("test")
		var countRun int
		var wg sync.WaitGroup
		plugins := inputest.SinglePlugin("test", inputest.ConstInputManager(&inputest.MockInput{
			OnRun: func(ctx v2.Context, _ beat.PipelineConnector) error {
				defer wg.Done()
				countRun++
				<-ctx.Cancelation.Done()
				return nil
			},
		}))
		loader := inputest.MustNewTestLoader(t, plugins, "type", "test")
		factory := RunnerFactory(log, beat.Info{}, loader.Loader)

		runner, err := factory.Create(nil, common.MustNewConfigFrom(map[string]interface{}{
			"type": "test",
		}))
		require.NoError(t, err)

		wg.Add(1)
		runner.Start()
		runner.Stop()
		wg.Wait()
		assert.Equal(t, 1, countRun)
	})

	t.Run("fail if input type is unknown to loader", func(t *testing.T) {
		log := logp.NewLogger("test")
		plugins := inputest.SinglePlugin("test", inputest.ConstInputManager(nil))
		loader := inputest.MustNewTestLoader(t, plugins, "type", "")
		factory := RunnerFactory(log, beat.Info{}, loader.Loader)

		// run
		runner, err := factory.Create(nil, common.MustNewConfigFrom(map[string]interface{}{
			"type": "unknown",
		}))
		assert.Nil(t, runner)
		assert.Error(t, err)
	})
}
