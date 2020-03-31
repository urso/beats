package v2

import (
	"fmt"

	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/feature"
	"github.com/urso/sderr"
)

type Collection interface {
	CreateService(prefix string) (BackgroundService, error)

	Find(name string) (Extension, error)

	Each(func(Extension) bool)
}

type Extension interface {
	Details() feature.Details
	Configure(cfg *common.Config) (Input, error)
}

type collectionList []Collection

func CombineCollections(regs ...Collection) Collection {
	return collectionList(regs)
}

// ValidateCollection checks if there are multiple extensions with the same name
// in the collection.
func ValidateCollection(c Collection) error {
	seen := common.StringSet{}
	dups := map[string]int{}

	// recursively look for duplicate entries.
	c.Each(func(ext Extension) bool {
		name := ext.Details().Name
		if seen.Has(name) {
			dups[name]++
		}
		seen.Add(name)
		return true
	})

	if len(dups) == 0 {
		return nil
	}

	var errs []error
	for name, count := range dups {
		errs = append(errs, fmt.Errorf("plugin '%v' found %v time(s)", name, count))
	}
	if len(errs) == 1 {
		return errs[0]
	}

	return sderr.WrapAll(errs, "registry has multiple duplicate plugins")
}

func (l collectionList) CreateService(prefix string) (BackgroundService, error) {
	var services []BackgroundService
	for _, c := range l {
		service, err := c.CreateService(prefix)
		if err != nil {
			return nil, err
		}

		services = append(services, service)
	}
	return CombineServices(services...), nil
}

func (l collectionList) Find(name string) (Extension, error) {
	var lastErr error
	for i := len(l) - 1; i != -1; i-- {
		c := l[i]
		ext, err := c.Find(name)
		if err == nil {
			return ext, err
		}

		lastErr = err
	}
	return nil, lastErr
}

func (l collectionList) Each(fn func(ext Extension) bool) {
	cont := true
	for i := 0; i < len(l) && cont; i++ {
		l[i].Each(func(ext Extension) bool {
			cont = fn(ext)
			return cont
		})
	}
}
