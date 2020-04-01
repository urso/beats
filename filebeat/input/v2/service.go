package v2

import (
	"github.com/elastic/go-concert/unison"
	"github.com/urso/sderr"
)

// BackgroundService can be used to run maintenance tasks in the
// background, even if no input is configured.
type BackgroundService interface {
	Run(cancel Canceler) error
}

type serviceList []BackgroundService

// CombineServices combinees a list of background services into a single
// BackgroundService.
func CombineServices(services ...BackgroundService) BackgroundService {
	return serviceList(services)
}

func (sl serviceList) Run(cancel Canceler) error {
	var group unison.MultiErrGroup
	for _, service := range sl {
		group.Go(func() error {
			return service.Run(cancel)
		})
	}

	if errs := group.Wait(); len(errs) > 0 {
		sderr.WrapAll(errs, "plugin background service failures")
	}
	return nil
}
