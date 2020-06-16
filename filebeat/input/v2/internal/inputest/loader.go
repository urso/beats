package inputest

import (
	"testing"

	v2 "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
)

type Loader struct {
	t testing.TB
	*v2.Loader
}

func MustNewTestLoader(t testing.TB, plugins []v2.Plugin, typeField, defaultType string) *Loader {
	l, err := v2.NewLoader(logp.NewLogger("test"), plugins, typeField, defaultType)
	if err != nil {
		t.Fatalf("Failed to create loader: %v", err)
	}
	return &Loader{t: t, Loader: l}
}

func (l *Loader) MustConfigure(cfg *common.Config) v2.Input {
	i, err := l.Configure(cfg)
	if err != nil {
		l.t.Fatalf("Failed to create the input: %v", err)
	}
	return i
}
