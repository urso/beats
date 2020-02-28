package v2

import "github.com/elastic/beats/v7/libbeat/common"

// Loader is used to create an Input instance. The input created
// only represents a configured object. The input MUST NOT start any
// processing yet.
type Loader interface {
	Configure(*common.Config) (Input, error)
}

// LoaderList combines multiple Loaders into one Loader.
type LoaderList []Loader

var _ Loader = (*LoaderList)(nil)

// Add adds another loader to the list.
// Warning: a loader list must not be added to itself (or cross references between ListLoaders),
//          so to not run into an infinite loop when trying to create an input.
func (l *LoaderList) Add(other Loader) {
	*l = append(*l, other)
}

// Configure asks each loader to create an input. The first loader that creates an
// input without an error wins. If an input with configuration error is
// returned we will hold on to it, reporting the last configuration
// error we have witnessed.
func (l LoaderList) Configure(cfg *common.Config) (Input, error) {
	var lastErr error
	var lastInput Input

	for _, loader := range l {
		input, err := loader.Configure(cfg)
		if input.Run != nil {
			if err == nil {
				return input, nil
			}

			lastInput = input
			lastErr = err
		} else if lastInput.Run == nil {
			lastErr = mergeLoadError(lastErr, err)
		}
	}

	return lastInput, lastErr
}

func mergeLoadError(err1, err2 error) error {
	if failedInputName(err1) != "" && failedInputName(err2) == "" {
		return err1
	}
	return err2
}
