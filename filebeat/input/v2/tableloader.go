package v2

/*

import (
	"fmt"
	"strings"

	"github.com/elastic/beats/v7/libbeat/common"
)

type tableLoader struct {
	typeField string
	table     map[string]ExtensionManager
}

// NewTableLoader creates a new ExtensionManager, that will select loaders based on the typeField.
func NewTableLoader(typeField string, optLoaders map[string]ExtensionManager) ExtensionManager {
	t := &tableLoader{
		typeField: typeFieldName(typeField),
		table:     make(map[string]ExtensionManager, len(optLoaders)),
	}
	for name, l := range optLoaders {
		t.table[name] = l
	}
	return t
}

func (l *tableLoader) Find(cfg *common.Config) (Extension, error) {
	fullName, err := getTypeName(cfg, l.typeField)
	if err != nil {
		return nil, err
	}

	var key string
	typeName := fullName
	idx := strings.IndexRune(typeName, '/')
	if idx >= 0 {
		key = typeName[:idx]
		typeName = typeName[idx+1:]
	}

	loader := l.table[key]
	if loader == nil {
		return nil, &LoaderError{
			Name:    fullName,
			Reason:  ErrUnknown,
			Message: fmt.Sprintf("no plugin namespace for '%v' defined", key),
		}
	}

	subConfig := cfg.Clone()
	subConfig.SetString(l.typeField, -1, typeName)
	return loader.Find(subConfig)
}

*/
