package v2

/*
import "github.com/elastic/beats/v7/libbeat/common"

// ConfigsLoader uses a transformer to generate the final configuration to be passed to a loader.
// The transformer can generate multiple configurations. An input for each configuration will be generated.
// The generated inputs will be combined into one common input that runs the given inputs concurrently.
type ConfigsLoader struct {
	// The field that will be used to select the type from the configuration.
	TypeField string

	// Transform transforms the configuration into a set of input configurations.
	// Each configuration will be passed to the given loader.
	Transform ConfigTransformer
	Manager   ExtensionManager

	// Strict requires the input type to be always passed to the transformer. If set ConfigsLoader will fail
	// if the type name is unknown to the Transformer. If Strict is set to false, then the configuration is passed
	// directly to the loader if the type name is unknown to the transformer.
	Strict bool

	// Rescursive, if set instructs the ConfigsLoader to feed generated configuration back to the Transformer.
	// This allows transformations to reference other transformation inputs. THe ConfigsLoader keeps track of used
	// names and will fail if it detects a loop.
	Recusrive bool
}

// ConfigTransformer creates multiple input configurations based on a given input configuration.
type ConfigTransformer interface {
	Has(name string) bool
	Transform(cfg *common.Config) ([]*common.Config, error)
}

type extensionList []Extension

var _ ExtensionManager = (*ConfigsLoader)(nil)

func (l *ConfigsLoader) Find(cfg *common.Config) (Extension, error) {
	if l.Transform == nil || l.Manager == nil {
		panic("invalid configs loader")
	}

	typeField := typeFieldName(l.TypeField)

	exts, err := l.find(typeField, nil, cfg)
	if err != nil {
		return nil, err
	}
	if len(exts) == 1 {
		return exts[0], nil
	}

	return extensionList(exts), err
}

func (l *ConfigsLoader) find(
	typeField string,
	visited []string,
	cfg *common.Config,
) ([]Extension, error) {
	inputName, err := getTypeName(cfg, typeField)
	if err != nil {
		return nil, err
	}

	// load root configuration
	if len(visited) == 0 {
		has := l.Transform.Has(inputName)
		if !has {
			if l.Strict {
				return nil, &LoaderError{Name: inputName, Reason: ErrUnknown}
			}
			return l.findDirect(cfg)
		}
		return l.findChildren(typeField, visited, inputName, cfg)
	}

	if !l.Recusrive {
		return l.findDirect(cfg)
	}

	if alreadySeen(visited, inputName) {
		return nil, &LoaderError{Name: inputName, Reason: ErrInfiniteLoadLoop}
	}

	if !l.Transform.Has(inputName) {
		return l.findDirect(cfg)
	}
	return l.findChildren(typeField, visited, inputName, cfg)
}

func (l *ConfigsLoader) findDirect(cfg *common.Config) ([]Extension, error) {
	ext, err := l.Manager.Find(cfg)
	if err != nil {
		return nil, err
	}
	return []Extension{ext}, err
}

func (l *ConfigsLoader) findChildren(
	typeField string,
	visited []string,
	name string,
	cfg *common.Config,
) ([]Extension, error) {
	cfgs, err := l.Transform.Transform(cfg)
	if err != nil {
		return nil, err
	}

	var exts []Extension
	visited = append(visited, name)
	for _, childCfg := range cfgs {
		children, err := l.find(typeField, visited, childCfg)
		if err != nil {
			return nil, &LoaderError{
				Name:    name,
				Reason:  err,
				Message: "failed to load child extension",
			}
		}

		exts = append(exts, children...)
	}
	return exts, nil
}

func alreadySeen(set []string, str string) bool {
	for _, s := range set {
		if s == str {
			return true
		}
	}
	return false
}

func (ext extensionList)

*/
