// Package pipetool provide helper functions for enhanceing a beat.Pipeline with
// custom functionality.
package pipetool

import (
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/common/fmtstr"
	"github.com/elastic/beats/v7/libbeat/processors"
	"github.com/elastic/beats/v7/libbeat/processors/add_formatted_index"
)

type connectEditPipeline struct {
	parent beat.PipelineConnector
	edit   func(beat.ClientConfig) beat.ClientConfig
}

type wrapClientPipeline struct {
	parent beat.PipelineConnector
	edit   func(beat.Client) beat.Client
}

type eventCounter interface {
	Add(n int)
	Done()
}

type countingClient struct {
	counter eventCounter
	client  beat.Client
}

// countingEventer adjusts wgEvents if events are dropped during shutdown.
type countingEventer struct {
	wgEvents eventCounter
}

type combinedEventer struct {
	a, b beat.ClientEventer
}

// commonInputConfig defines common input settings
// for the publisher pipeline.
type commonInputConfig struct {
	// event processing
	common.EventMetadata `config:",inline"`      // Fields and tags to add to events.
	Processors           processors.PluginConfig `config:"processors"`
	KeepNull             bool                    `config:"keep_null"`

	// implicit event fields
	Type        string `config:"type"`         // input.type
	ServiceType string `config:"service.type"` // service.type

	// hidden filebeat modules settings
	Module  string `config:"_module_name"`  // hidden setting
	Fileset string `config:"_fileset_name"` // hidden setting

	// Output meta data settings
	Pipeline string                   `config:"pipeline"` // ES Ingest pipeline name
	Index    fmtstr.EventFormatString `config:"index"`    // ES output index pattern
}

// WithClientConfig reads common Beat input instance configurations from the
// configuration object and ensure that the settings are applied to each client.
//
// Common settings:
//  - *fields*: common fields to be added to the pipeline
//  - *fields_under_root*: select at which level to store the fields
//  - *tags*: add additional tags to the events
//  - *processors*: list of local processors to be added to the processing pipeline
//  - *keep_null*: keep or remove 'null' from events to be published
//  - *_module_name* (hidden setting): Add fields describing the module name
//  - *_ fileset_name* (hiddrn setting):
//  - *pipeline*: Configure the ES Ingest Node pipeline name to be used for events from this input
//  - *index*: Configure the index name for events to be collected from this input
//  - *type*: implicit event type
//  - *service.type*: implicit event type
func WithClientConfig(
	beatInfo beat.Info,
	pipeline beat.PipelineConnector,
	cfg *common.Config,
) (beat.PipelineConnector, error) {
	editor, err := newCommonConfigEditor(beatInfo, cfg)
	if err != nil {
		return nil, err
	}
	return WithClientConfigEdit(pipeline, editor), nil
}

func newCommonConfigEditor(
	beatInfo beat.Info,
	cfg *common.Config,
) (func(beat.ClientConfig) beat.ClientConfig, error) {
	config := commonInputConfig{}
	if err := cfg.Unpack(&config); err != nil {
		return nil, err
	}

	var indexProcessor processors.Processor
	if !config.Index.IsEmpty() {
		staticFields := fmtstr.FieldsForBeat(beatInfo.Beat, beatInfo.Version)
		timestampFormat, err :=
			fmtstr.NewTimestampFormatString(&config.Index, staticFields)
		if err != nil {
			return nil, err
		}
		indexProcessor = add_formatted_index.New(timestampFormat)
	}

	userProcessors, err := processors.New(config.Processors)
	if err != nil {
		return nil, err
	}

	serviceType := config.ServiceType
	if serviceType == "" {
		serviceType = config.Module
	}

	return func(clientCfg beat.ClientConfig) beat.ClientConfig {
		meta := clientCfg.Processing.Meta.Clone()
		fields := clientCfg.Processing.Fields.Clone()

		setOptional(meta, "pipeline", config.Pipeline)
		setOptional(fields, "fileset.name", config.Fileset)
		setOptional(fields, "service.type", serviceType)
		setOptional(fields, "input.type", config.Type)
		if config.Module != "" {
			event := common.MapStr{"module": config.Module}
			if config.Fileset != "" {
				event["dataset"] = config.Module + "." + config.Fileset
			}
			fields["event"] = event
		}

		// assemble the processors. Ordering is important.
		// 1. add support for index configuration via processor
		// 2. add processors added by the input that wants to connect
		// 3. add locally configured processors from the 'processors' settings
		procs := processors.NewList(nil)
		if indexProcessor != nil {
			procs.AddProcessor(indexProcessor)
		}
		if lst := clientCfg.Processing.Processor; lst != nil {
			procs.AddProcessor(lst)
		}
		if userProcessors != nil {
			procs.AddProcessors(*userProcessors)
		}

		clientCfg.Processing.EventMetadata = config.EventMetadata
		clientCfg.Processing.Meta = meta
		clientCfg.Processing.Fields = fields
		clientCfg.Processing.Processor = procs
		clientCfg.Processing.KeepNull = config.KeepNull

		return clientCfg
	}, nil
}

// WithDefaultGuarantee sets the default sending guarantee to `mode` if the
// beat.ClientConfig does not set the mode explicitly.
func WithDefaultGuarantees(pipeline beat.PipelineConnector, mode beat.PublishMode) beat.PipelineConnector {
	return WithClientConfigEdit(pipeline, func(cfg beat.ClientConfig) beat.ClientConfig {
		if cfg.PublishMode == beat.DefaultGuarantees {
			cfg.PublishMode = mode
		}
		return cfg
	})
}

// WithClientConfigEdit creates a pipeline connector that support developers to modify the given beat.ClientConfig
// before connecting to the pipeline. This way a beat/module can prepare common settings that must be applied to all
// inputs
func WithClientConfigEdit(pipeline beat.PipelineConnector, fn func(beat.ClientConfig) beat.ClientConfig) beat.PipelineConnector {
	return &connectEditPipeline{parent: pipeline, edit: fn}
}

func (p *connectEditPipeline) Connect() (beat.Client, error) {
	return p.ConnectWith(beat.ClientConfig{})
}

func (p *connectEditPipeline) ConnectWith(cfg beat.ClientConfig) (beat.Client, error) {
	return p.parent.ConnectWith(p.edit(cfg))
}

func setOptional(to common.MapStr, key string, value string) {
	if value != "" {
		to.Put(key, value)
	}
}

func WithClientWrapper(pipeline beat.PipelineConnector, fn func(beat.Client) beat.Client) beat.PipelineConnector {
	return &wrapClientPipeline{parent: pipeline, edit: fn}
}

func (p *wrapClientPipeline) Connect() (beat.Client, error) {
	return p.ConnectWith(beat.ClientConfig{})
}

func (p *wrapClientPipeline) ConnectWith(cfg beat.ClientConfig) (beat.Client, error) {
	client, err := p.parent.ConnectWith(cfg)
	if err == nil {
		client = p.edit(client)
	}
	return client, err
}

// WithEventCounter adds a counter to the pipeline that keeps track of all events
// published and acked by each client in that has been connected.
func WithEventCounter(pipeline beat.PipelineConnector, counter eventCounter) beat.PipelineConnector {
	counterListener := &countingEventer{counter}

	pipeline = WithClientConfigEdit(pipeline, func(config beat.ClientConfig) beat.ClientConfig {
		if evts := config.Events; evts != nil {
			config.Events = &combinedEventer{evts, counterListener}
		} else {
			config.Events = counterListener
		}
		return config
	})

	pipeline = WithClientWrapper(pipeline, func(client beat.Client) beat.Client {
		return &countingClient{
			counter: counter,
			client:  client,
		}
	})

	return pipeline
}

func (*countingEventer) Closing()   {}
func (*countingEventer) Closed()    {}
func (*countingEventer) Published() {}

func (c *countingEventer) FilteredOut(_ beat.Event) {}
func (c *countingEventer) DroppedOnPublish(_ beat.Event) {
	c.wgEvents.Done()
}

func (c *combinedEventer) Closing() {
	c.a.Closing()
	c.b.Closing()
}

func (c *combinedEventer) Closed() {
	c.a.Closed()
	c.b.Closed()
}

func (c *combinedEventer) Published() {
	c.a.Published()
	c.b.Published()
}

func (c *combinedEventer) FilteredOut(event beat.Event) {
	c.a.FilteredOut(event)
	c.b.FilteredOut(event)
}

func (c *combinedEventer) DroppedOnPublish(event beat.Event) {
	c.a.DroppedOnPublish(event)
	c.b.DroppedOnPublish(event)
}

func (c *countingClient) Publish(event beat.Event) {
	c.counter.Add(1)
	c.client.Publish(event)
}

func (c *countingClient) PublishAll(events []beat.Event) {
	c.counter.Add(len(events))
	c.client.PublishAll(events)
}

func (c *countingClient) Close() error {
	return c.client.Close()
}
