package compat

import (
	"sync"
	
	"github.com/elastic/go-concert"

	v2 "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/cfgfile"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
)

type factory struct {
	log    *logp.Logger
	info   beat.Info
	loader v2.Loader
}

type runner struct {
	log       *logp.Logger
	agent     *beat.Info
	wg        sync.WaitGroup
	sig       concert.OnceSignaler
	input     v2.Input
	connector beat.PipelineConnector
	meta      *common.MapStrPointer
}

// RunnerFactory create a runner factory on top of input Loaders
// that is compatible with cfgfile runners and autodiscovery.
func RunnerFactory(
	log *logp.Logger,
	info beat.Info,
	loader v2.Loader,
) cfgfile.CheckableRunnerFactory {
	return &factory{log: log, info: info, loader: loader}
}

func (f *factory) CheckConfig(config *common.Config) error {
	_, err := f.loader.Configure(c)
	if err != nil {
		return err
	}
	return nil
}

func (f *factory) Create(
	p beat.Pipeline,
	config *common.Config,
	meta *common.MapStrPointer,
) (cfgfile.Runner, error) {
	input, err := f.loader.Configure(config)
	if err != nil {
		return nil, err
	}

	return &runner{
		log:       f.log.Named(input.Name),
		agent:     &f.info,
		sig:       concert.NewOnceSignaler(),
		input:     input,
		connector: p,
		meta:      meta,
	}, nil
}

func (r *runner) String() string { return r.input.Name }
func (r *runner) Start() {
	log := r.log
	name := r.input.Name

	status := v2.OptStatusObserver{
		Initialized: func() {
			log.Debugf("Input '%v' initialized", name)
		},
		Active: func() {
			log.Debugf("Input '%v' active", name)
		},
		Failing: func(err error) {
			log.Errorf("Input '%v' in temporary error state caused by: %+v",
				name, err)
		},
		Stopping: func() {
			log.Infof("Input '%v' received stop signal.", name)
		},
	}.Create()

	go func() {
		log.Infof("Input %v starting", name)
		err := r.input.Run(
			v2.Context{
				ID:          "", // TODO: hmmm....
				Agent:       *r.agent,
				Logger:      log,
				Cancelation: r.sig,
				Status:      status,
				Metadata:    r.meta,
			},
			r.connector,
		)
		if err != nil {
			log.Errorf("Input '%v' failed with: %+v", name, err)
		}
	}()
}

func (r *runner) Stop() {
	r.sig.Trigger()
	r.wg.Wait()
	log.Infof("Input '%v' stopped", r.input.Name)
}
