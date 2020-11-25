package esbulk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/NYTimes/gziphandler"

	v2 "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/beat/events"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/go-concert/ctxtool"
)

type esbulkInput struct {
	address string
	version string
}

type actionMeta struct {
	Index    string `json:"_index"`
	ID       string `json:"_id"`
	Pipeline string `json:"pipeline"`
}

func newESBulkInput(version string, settings settings) (*esbulkInput, error) {
	return &esbulkInput{
		address: settings.Address,
		version: version,
	}, nil
}

// Name reports the input name.
func (*esbulkInput) Name() string {
	return pluginName
}

// Test checks the configuaration and runs additional checks if the Input can
// actually collect data for the given configuration (e.g. check if host/port or files are
// accessible).
func (*esbulkInput) Test(_ v2.TestContext) error {
	return nil
}

// Run starts the data collection. Run must return an error only if the
// error is fatal making it impossible for the input to recover.
func (inp *esbulkInput) Run(ctx v2.Context, pipeline beat.Pipeline) error {
	httpBaseCtx := ctxtool.FromCanceller(ctx.Cancelation)

	reporter, err := pipeline.ConnectWith(beat.ClientConfig{
		ACKHandler: newPipelineACKer(),
	})
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.Handle("/", gziphandler.GzipHandler(handleRoot(inp.version)))
	mux.Handle("/_bulk", gziphandler.GzipHandler(handleBulk(ctx.Logger, reporter)))
	server := &http.Server{
		Addr:    inp.address,
		Handler: mux,
		BaseContext: func(_ net.Listener) context.Context {
			return httpBaseCtx
		},
	}

	_, cancel := ctxtool.WithFunc(httpBaseCtx, func() {
		server.Close()
	})
	defer cancel()

	return server.ListenAndServe()
}

func handleRoot(version string) http.HandlerFunc {
	return func(resp http.ResponseWriter, req *http.Request) {
		fmt.Fprintf(resp, "{\"version\": { \"number\": \"%v\" }}", version)
	}
}

func handleBulk(logger *logp.Logger, reporter beat.Client) http.HandlerFunc {
	type itemResult struct {
		// TODO: we need to distinguish the reponse by operation type
		Create struct {
			Status int `json:"status"`
		} `json:"create"`
	}

	type result struct {
		Items []itemResult `json:"items"`
	}

	return handleErr(func(resp http.ResponseWriter, req *http.Request) error {
		count := 0
		dec := json.NewDecoder(req.Body)
		defer req.Body.Close()

		bulkACKer := newBulkACKHandler()

		var itemResults []itemResult

		for dec.More() {
			var action map[string]actionMeta
			if err := dec.Decode(&action); err != nil {
				return err
			}

			if !dec.More() {
				return errors.New("Document missing")
			}

			var document common.MapStr
			if err := dec.Decode(&document); err != nil {
				return err
			}

			var result itemResult
			result.Create.Status = http.StatusOK
			if err := publishEvent(reporter, bulkACKer, action, document); err != nil {
				result.Create.Status = http.StatusBadRequest
			}

			itemResults = append(itemResults, result)
			count++
		}

		resp.Header().Set("Content-Type", "application/json")
		resp.WriteHeader(http.StatusOK)

		if err := bulkACKer.wait(req.Context()); err != nil {
			return err
		}

		enc := json.NewEncoder(resp)
		enc.Encode(result{itemResults})
		return nil
	})
}

func handleErr(fn func(http.ResponseWriter, *http.Request) error) http.HandlerFunc {
	return func(resp http.ResponseWriter, req *http.Request) {
		err := fn(resp, req)
		if err == nil || err == context.Canceled || err == context.DeadlineExceeded {
			return
		}

		msg := struct{ Error string }{err.Error()}
		b, _ := json.Marshal(msg)
		resp.Header().Set("Content-Type", "application/json")
		resp.WriteHeader(http.StatusBadRequest)
		resp.Write(b)
	}
}

func publishEvent(
	reporter beat.Client,
	acker *bulkACKHandler,
	action map[string]actionMeta,
	document common.MapStr,
) error {
	event, err := createEvent(action, document)
	if err != nil {
		return err
	}

	event.Private = acker.addEvent()
	reporter.Publish(event)
	return nil
}

func createEvent(action map[string]actionMeta, document common.MapStr) (beat.Event, error) {
	if len(action) != 1 {
		return beat.Event{}, fmt.Errorf("invalid bulk action: %v", action)
	}

	var actionMeta actionMeta
	var actionOp string
	for op, m := range action {
		actionMeta = m
		actionOp = op
	}

	var meta common.MapStr
	meta = addOptMeta(meta, events.FieldMetaOpType, actionOp)
	meta = addOptMeta(meta, events.FieldMetaID, actionMeta.ID)
	meta = addOptMeta(meta, events.FieldMetaIndex, actionMeta.Index)
	meta = addOptMeta(meta, events.FieldMetaPipeline, actionMeta.Pipeline)

	return beat.Event{
		Timestamp: extractTimestamp(document),
		Meta:      meta,
		Fields:    document,
	}, nil
}

func addOptMeta(meta common.MapStr, key string, value string) common.MapStr {
	if value == "" {
		return meta
	}

	if meta == nil {
		return common.MapStr{key: value}
	}

	meta[key] = value
	return meta
}

func extractTimestamp(document common.MapStr) time.Time {
	if tsRaw, ok := document["@timestamp"]; ok {
		delete(document, "@timestamp")
		if tsString, ok := tsRaw.(string); ok {
			if parsed, err := common.ParseTime(tsString); err == nil {
				return time.Time(parsed)
			}
		}
	}

	return time.Now()
}
