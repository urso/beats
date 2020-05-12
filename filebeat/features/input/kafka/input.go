package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/urso/sderr"

	input "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/common/acker"
	"github.com/elastic/beats/v7/libbeat/common/backoff"
	"github.com/elastic/beats/v7/libbeat/common/kafka"
	"github.com/elastic/beats/v7/libbeat/feature"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/go-concert/ctxtool"
)

type kafkaInput struct {
	config       kafkaInputConfig
	saramaConfig *sarama.Config
}

// The group handler for the sarama consumer group interface. In addition to
// providing the basic consumption callbacks needed by sarama, groupHandler is
// also currently responsible for marshalling kafka messages into beat.Event,
// and passing ACKs from the output channel back to the kafka cluster.
type groupHandler struct {
	sync.Mutex
	version kafka.Version
	session sarama.ConsumerGroupSession
	out     beat.Client
	// if the fileset using this input expects to receive multiple messages bundled under a specific field then this value is assigned
	// ex. in this case are the azure fielsets where the events are found under the json object "records"
	expandEventListFromField string
	log                      *logp.Logger
}

// The metadata attached to incoming events so they can be ACKed once they've
// been successfully sent.
type eventMeta struct {
	handler *groupHandler
	message *sarama.ConsumerMessage
}

const pluginName = "kafka"

func Plugin() input.Plugin {
	return input.Plugin{
		Name:       pluginName,
		Stability:  feature.Stable,
		Deprecated: false,
		Info:       "kafka input",
		Doc:        "Collect events from Kafka topics",
		Manager:    &inputManager{Configure: configure},
	}
}

func configure(cfg *common.Config) (input.Input, error) {
	config := defaultConfig()
	if err := cfg.Unpack(&config); err != nil {
		return nil, errors.Wrap(err, "reading kafka input config")
	}

	saramaConfig, err := newSaramaConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "initializing Sarama config")
	}

	return &kafkaInput{
		config:       config,
		saramaConfig: saramaConfig,
	}, nil
}

func (inp *kafkaInput) Name() string { return pluginName }

func (inp *kafkaInput) Test(ctx input.TestContext) error {
	grp, err := sarama.NewConsumerGroup(inp.config.Hosts, inp.config.GroupID, inp.saramaConfig)
	if err != nil {
		return sderr.Wrap(err, "failed to initialize kafka consumer group '%{group}'", inp.config.GroupID)
	}
	return grp.Close()
}

func (inp *kafkaInput) Run(
	ctx input.Context,
	pipeline beat.PipelineConnector,
) error {
	ctx.Logger = ctx.Logger.With("group", inp.config.GroupID)

	client, err := pipeline.ConnectWith(beat.ClientConfig{
		ACKHandler: acker.ConnectionOnly(
			acker.EventPrivateReporter(func(_ int, events []interface{}) {
				for _, event := range events {
					if meta, ok := event.(eventMeta); ok {
						meta.handler.ack(meta.message)
					}
				}
			}),
		),
		CloseRef:  ctx.Cancelation,
		WaitClose: inp.config.WaitClose,
	})
	if err != nil {
		return err
	}
	defer client.Close()

	// If the consumer fails to connect, we use exponential backoff with
	// jitter up to 8 * the initial backoff interval.
	backoff := backoff.NewEqualJitterBackoff(
		ctx.Cancelation.Done(),
		inp.config.ConnectBackoff,
		8*inp.config.ConnectBackoff)

	for ctx.Cancelation.Err() == nil {
		consumerGroup, err := sarama.NewConsumerGroup(
			inp.config.Hosts, inp.config.GroupID, inp.saramaConfig)
		if err != nil {
			ctx.Logger.Errorw(
				"Error initializing kafka consumer group", "error", err)
			backoff.Wait()
			continue
		}
		// We've successfully connected, reset the backoff timer.
		backoff.Reset()

		inp.runConsumerGroup(ctx, client, consumerGroup)
	}

	return nil
}

func (input *kafkaInput) runConsumerGroup(
	ctx input.Context,
	out beat.Client,
	consumerGroup sarama.ConsumerGroup,
) {
	handler := &groupHandler{
		version: input.config.Version,
		out:     out,
		// expandEventListFromField will be assigned the configuration option expand_event_list_from_field
		expandEventListFromField: input.config.ExpandEventListFromField,
		log:                      ctx.Logger,
	}

	defer consumerGroup.Close()

	// Listen asynchronously to any errors during the consume process
	go func() {
		for err := range consumerGroup.Errors() {
			ctx.Logger.Errorw("Error reading from kafka", "error", err)
		}
	}()

	err := consumerGroup.Consume(ctxtool.FromCanceller(ctx.Cancelation), input.config.Topics, handler)
	if err != nil && err != context.Canceled {
		ctx.Logger.Errorw("Kafka consume error", "error", err)
	}
}

func (h *groupHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.Lock()
	h.session = session
	h.Unlock()
	return nil
}

func (h *groupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	h.Lock()
	h.session = nil
	h.Unlock()
	return nil
}

// ack informs the kafka cluster that this message has been consumed. Called
// from the input's ACKEvents handler.
func (h *groupHandler) ack(message *sarama.ConsumerMessage) {
	h.Lock()
	defer h.Unlock()
	if h.session != nil {
		h.session.MarkMessage(message, "")
	}
}

func (h *groupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		events := h.createEvents(sess, claim, msg)
		for _, event := range events {
			h.out.Publish(event)
		}
	}
	return nil
}

// parseMultipleMessages will try to split the message into multiple ones based on the group field provided by the configuration
func (h *groupHandler) parseMultipleMessages(bMessage []byte) []string {
	var obj map[string][]interface{}
	err := json.Unmarshal(bMessage, &obj)
	if err != nil {
		h.log.Errorw(fmt.Sprintf("Kafka desirializing multiple messages using the group object %s", h.expandEventListFromField), "error", err)
		return []string{}
	}
	var messages []string
	if len(obj[h.expandEventListFromField]) > 0 {
		for _, ms := range obj[h.expandEventListFromField] {
			js, err := json.Marshal(ms)
			if err == nil {
				messages = append(messages, string(js))
			} else {
				h.log.Errorw(fmt.Sprintf("Kafka serializing message %s", ms), "error", err)
			}
		}
	}
	return messages
}

func (h *groupHandler) createEvents(
	sess sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
	message *sarama.ConsumerMessage,
) []beat.Event {
	timestamp := time.Now()
	kafkaFields := common.MapStr{
		"topic":     claim.Topic(),
		"partition": claim.Partition(),
		"offset":    message.Offset,
		"key":       string(message.Key),
	}

	version, versionOk := h.version.Get()
	if versionOk && version.IsAtLeast(sarama.V0_10_0_0) {
		timestamp = message.Timestamp
		if !message.BlockTimestamp.IsZero() {
			kafkaFields["block_timestamp"] = message.BlockTimestamp
		}
	}
	if versionOk && version.IsAtLeast(sarama.V0_11_0_0) {
		kafkaFields["headers"] = arrayForKafkaHeaders(message.Headers)
	}

	// if expandEventListFromField has been set, then a check for the actual json object will be done and a return for multiple messages is executed
	var events []beat.Event
	var messages []string
	if h.expandEventListFromField == "" {
		messages = []string{string(message.Value)}
	} else {
		messages = h.parseMultipleMessages(message.Value)
	}
	for _, msg := range messages {
		event := beat.Event{
			Timestamp: timestamp,
			Fields: common.MapStr{
				"message": msg,
				"kafka":   kafkaFields,
			},
			Private: eventMeta{
				handler: h,
				message: message,
			},
		}
		events = append(events, event)

	}
	return events
}

func arrayForKafkaHeaders(headers []*sarama.RecordHeader) []string {
	array := []string{}
	for _, header := range headers {
		// Rather than indexing headers in the same object structure Kafka does
		// (which would give maximal fidelity, but would be effectively unsearchable
		// in elasticsearch and kibana) we compromise by serializing them all as
		// strings in the form "<key>: <value>". For this we need to mask
		// occurrences of ":" in the original key, which we expect to be uncommon.
		// We may consider another approach in the future when it's more clear what
		// the most common use cases are.
		key := strings.ReplaceAll(string(header.Key), ":", "_")
		value := string(header.Value)
		array = append(array, fmt.Sprintf("%s: %s", key, value))
	}
	return array
}
