package compat

import (
	"errors"
	"testing"

	v2 "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/cfgfile"
	"github.com/elastic/beats/v7/libbeat/common"
)

func TestCombine_CheckConfig(t *testing.T) {
	oops1 := errors.New("oops1")
	oops2 := errors.New("oops2")

	cases := map[string]struct {
		factory, fallback cfgfile.RunnerFactory
		want              error
	}{
		"success": {
			factory:  failingRunnerFactory(nil),
			fallback: failingRunnerFactory(nil),
			want:     nil,
		},
		"fail if factory fails already": {
			factory:  failingRunnerFactory(oops1),
			fallback: failingRunnerFactory(oops2),
			want:     oops1,
		},
		"do not fail in fallback if factory is fine": {
			factory:  failingRunnerFactory(nil),
			fallback: failingRunnerFactory(oops2),
			want:     nil,
		},
		"ignore ErrUnknown and use check from fallback": {
			factory:  failingRunnerFactory(v2.ErrUnknown),
			fallback: failingRunnerFactory(oops2),
			want:     oops2,
		},
	}

	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			factory := Combine(test.factory, test.fallback)
			cfg := common.MustNewConfigFrom(struct{ Type string }{"test"})
			err := factory.CheckConfig(cfg)
			if test.want != err {
				t.Fatalf("Failed. Want: %v, Got: %v", test.want, err)
			}
		})
	}

}

func TestCombine_Create(t *testing.T) {
	type validation func(*testing.T, cfgfile.Runner, error)

	wantError := func(want error) validation {
		return func(t *testing.T, _ cfgfile.Runner, got error) {
			if want != got {
				t.Fatalf("Wrong error. Want: %v, Got: %v", want, got)
			}
		}
	}

	wantRunner := func(want cfgfile.Runner) validation {
		return func(t *testing.T, got cfgfile.Runner, err error) {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if got != want {
				t.Fatalf("Wrong runner. Want: %v Got: %v", want, got)
			}
		}
	}

	runner1 := &fakeRunner{Name: "runner1"}
	runner2 := &fakeRunner{Name: "runner2"}
	oops1 := errors.New("oops1")
	oops2 := errors.New("oops2")

	cases := map[string]struct {
		factory  cfgfile.RunnerFactory
		fallback cfgfile.RunnerFactory
		Type     string
		check    validation
	}{
		"runner exsits in factory only": {
			factory:  constRunnerFactory(runner1),
			fallback: failingRunnerFactory(oops2),
			check:    wantRunner(runner1),
		},
		"runner exists in fallback only": {
			factory:  failingRunnerFactory(v2.ErrUnknown),
			fallback: constRunnerFactory(runner2),
			check:    wantRunner(runner2),
		},
		"runner from factory has higher priority": {
			factory:  constRunnerFactory(runner1),
			fallback: constRunnerFactory(runner2),
			check:    wantRunner(runner1),
		},
		"if both fail return error from factory": {
			factory:  failingRunnerFactory(oops1),
			fallback: failingRunnerFactory(oops2),
			check:    wantError(oops1),
		},
		"ignore ErrUnknown": {
			factory:  failingRunnerFactory(v2.ErrUnknown),
			fallback: failingRunnerFactory(oops2),
			check:    wantError(oops2),
		},
	}

	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			factory := Combine(test.factory, test.fallback)
			cfg := common.MustNewConfigFrom(struct{ Type string }{test.Type})
			runner, err := factory.Create(nil, cfg)
			test.check(t, runner, err)
		})
	}
}
