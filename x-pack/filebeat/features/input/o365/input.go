package o365

import (
	"time"

	"github.com/Azure/go-autorest/autorest"
	input "github.com/elastic/beats/v7/filebeat/input/v2"
	cursor "github.com/elastic/beats/v7/filebeat/input/v2/input-cursor"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/common/useragent"
	"github.com/elastic/beats/v7/libbeat/feature"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/x-pack/filebeat/input/o365audit/auth"
	"github.com/elastic/beats/v7/x-pack/filebeat/input/o365audit/poll"
	"github.com/elastic/go-concert/ctxtool"
	"github.com/pkg/errors"
)

const pluginName = "o365audit"

type o365 struct {
	config Config
}

// Stream represents an event stream.
type stream struct {
	tenantID, contentType string
	tokenProvider         auth.TokenProvider
}

func Plugin(log *logp.Logger, store cursor.StateStore) input.Plugin {
	return input.Plugin{
		Name:       pluginName,
		Stability:  feature.Experimental,
		Deprecated: false,
		Info:       "O365 logs",
		Doc:        "Collect logs from O365 service",
		Manager: &cursor.InputManager{
			Logger:     log,
			StateStore: store,
			Type:       pluginName,
			Configure:  configure,
		},
	}
}

func configure(cfg *common.Config) ([]cursor.Source, cursor.Input, error) {
	config := defaultConfig()
	if err := cfg.Unpack(&config); err != nil {
		return nil, nil, errors.Wrap(err, "reading config")
	}

	var sources []cursor.Source
	for _, tenantID := range config.TenantID {
		auth, err := config.NewTokenProvider(tenantID)
		if err != nil {
			return nil, nil, err
		}

		for _, contentType := range config.ContentType {
			sources = append(sources, &stream{
				tenantID:      tenantID,
				contentType:   contentType,
				tokenProvider: auth,
			})
		}
	}

	return sources, &o365{config: config}, nil
}

func (s *stream) Name() string {
	return s.tenantID + "::" + s.contentType
}

func (inp *o365) Name() string {
	return "o365auth"
}

func (inp *o365) Test(src cursor.Source, ctx input.TestContext) error {
	stream := src.(*stream)
	if _, err := stream.tokenProvider.Token(); err != nil {
		return errors.Wrapf(err, "unable to acquire authentication token for tenant:%s", stream.tenantID)
	}

	return nil
}

func (inp *o365) Run(
	ctx input.Context,
	src cursor.Source,
	cursor cursor.Cursor,
	publisher cursor.Publisher,
) error {
	stream := src.(*stream)
	if _, err := stream.tokenProvider.Token(); err != nil {
		return errors.Wrapf(err, "unable to acquire authentication token for tenant:%s", stream.tenantID)
	}

	config := &inp.config
	tenantID := stream.tenantID
	contentType := stream.contentType

	// MaxRequestsPerMinute limitation is per tenant.
	delay := time.Duration(len(config.ContentType)) * time.Minute / time.Duration(config.API.MaxRequestsPerMinute)

	log := ctx.Logger.With("tenantID", tenantID, "contentType", contentType)

	poller, err := poll.New(
		poll.WithTokenProvider(stream.tokenProvider),
		poll.WithMinRequestInterval(delay),
		poll.WithLogger(log),
		poll.WithContext(ctxtool.FromCanceller(ctx.Cancelation)),
		poll.WithRequestDecorator(
			autorest.WithUserAgent(useragent.UserAgent("Filebeat-"+pluginName)),
			autorest.WithQueryParameters(common.MapStr{
				"publisherIdentifier": tenantID,
			}),
		),
	)
	if err != nil {
		return errors.Wrap(err, "failed to create API poller")
	}

	start := makeCheckpoint(log, cursor, config.API.MaxRetention)
	action := makeListBlob(start, apiEnvironment{
		Logger:      log,
		TenantID:    tenantID,
		ContentType: contentType,
		Config:      inp.config.API,
		Callback:    publisher.Publish,
		Clock:       time.Now,
	})

	log.Infow("Start fetching events", "cursor", start)
	return poller.Run(action)
}
