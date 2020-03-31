package v2

import (
	"fmt"

	"github.com/elastic/beats/v7/libbeat/common"
)

type Loader struct {
	registry    Collection
	typeField   string
	defaultType string
}

func NewLoader(registry Collection, typeField, defaultType string) (*Loader, error) {
	required(registry != nil, "no registry set")
	if typeField == "" {
		typeField = "type"
	}

	if err := ValidateCollection(registry); err != nil {
		return nil, err
	}

	return &Loader{
		registry:    registry,
		typeField:   typeField,
		defaultType: defaultType,
	}, nil
}

func (l *Loader) Configure(cfg *common.Config) (Input, error) {
	name, err := cfg.String(l.typeField, -1)
	if err != nil {
		if l.defaultType == "" {
			return nil, &LoaderError{
				Reason:  ErrNoInputConfigured,
				Message: fmt.Sprintf("%v setting is missing", l.typeField),
			}
		}
		name = l.defaultType
	}

	ext, err := l.registry.Find(name)
	if err != nil {
		return nil, &LoaderError{Name: name, Reason: err}
	}

	return ext.Configure(cfg)
}
