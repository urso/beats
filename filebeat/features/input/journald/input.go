package journald

import (
	"os"
	"time"

	"github.com/coreos/go-systemd/sdjournal"
	input "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/common/backoff"
	"github.com/elastic/beats/v7/libbeat/feature"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/registry"
	"github.com/urso/sderr"
)

type journald struct {
	Backoff            time.Duration
	MaxBackoff         time.Duration
	Seek               seekMode
	CursorSeekFallback seekMode
	Matches            []matcher
	SaveRemoteHostname bool
}

type checkpoint struct {
	Version            int
	Position           string
	RealtimeTimestamp  uint64
	MonotonicTimestamp uint64
}

const (
	// LocalSystemJournalID is the ID of the local system journal.
	localSystemJournalID = "LOCAL_SYSTEM_JOURNAL"
)

func Plugin(log *logp.Logger, reg *registry.Registry, defaultStore string) input.Plugin {
	return input.Plugin{
		Name:       "journald",
		Stability:  feature.Beta,
		Deprecated: false,
		Info:       "journald input",
		Doc:        "The journald input collects logs from the local journald service",
		Manager: &CursorInputManager{
			Logger:       log,
			Registry:     reg,
			DefaultStore: defaultStore,
			Type:         "journald",
			Configure:    configure,
		},
	}
}

type pathSource string

var cursorVersion = 1

func (p pathSource) Name() string { return string(p) }

func configure(cfg *common.Config) ([]Source, CursorInput, error) {
	config := defaultConfig()
	if err := cfg.Unpack(&config); err != nil {
		return nil, nil, err
	}

	paths := config.Paths
	if len(paths) == 0 {
		paths = []string{localSystemJournalID}
	}

	sources := make([]Source, len(paths))
	for i, p := range paths {
		sources[i] = pathSource(p)
	}

	return sources, &journald{
		Backoff:            config.Backoff,
		MaxBackoff:         config.MaxBackoff,
		Seek:               config.Seek,
		CursorSeekFallback: config.CursorSeekFallback,
		Matches:            config.Matches,
		SaveRemoteHostname: config.SaveRemoteHostname,
	}, nil
}

func (inp *journald) Test(src Source, ctx input.TestContext) error {
	// 1. check if we can open the journal
	j, err := openJournal(src.Name())
	if err != nil {
		return err
	}
	defer j.Close()

	// 2. check if we can apply the configured filters
	if err := applyMatchers(j, inp.Matches); err != nil {
		return sderr.Wrap(err, "failed to apply filters to the %{path} journal", src.Name())
	}

	return nil
}

func (inp *journald) Run(
	ctx input.Context,
	src Source,
	cursor Cursor,
	publish func(beat.Event, interface{}) error,
) error {
	log := ctx.Logger.With("path", src.Name())
	checkpoint := initCheckpoint(log, cursor)

	j, err := openJournal(src.Name())
	if err != nil {
		return err
	}
	defer j.Close()

	if err := applyMatchers(j, inp.Matches); err != nil {
		return sderr.Wrap(err, "failed to apply filters to the %{path} journal", src.Name())
	}

	reader := &reader{
		log:     ctx.Logger,
		journal: j,
		backoff: backoff.NewExpBackoff(ctx.Cancelation.Done(), inp.Backoff, inp.MaxBackoff),
	}
	seekJournal(log, reader, checkpoint, inp.Seek, inp.CursorSeekFallback)

	converter := eventConverter{
		log:                log,
		saveRemoteHostname: inp.SaveRemoteHostname,
	}

	for {
		entry, err := reader.Next(ctx.Cancelation)
		if err != nil {
			return err
		}

		event := converter.Convert(entry)
		checkpoint.Position = entry.Cursor
		checkpoint.RealtimeTimestamp = entry.RealtimeTimestamp
		checkpoint.MonotonicTimestamp = entry.MonotonicTimestamp

		if err := publish(event, checkpoint); err != nil {
			return err
		}
	}
}

func initCheckpoint(log *logp.Logger, c Cursor) checkpoint {
	if c.IsNew() {
		return checkpoint{Version: cursorVersion}
	}

	var cp checkpoint
	err := c.Unpack(&cp)
	if err != nil {
		log.Errorf("Reset journald position. Failed to read checkpoint from registry: %v", err)
		return checkpoint{Version: cursorVersion}
	}

	if cp.Version != cursorVersion {
		log.Error("Reset journald position. invalid journald position entry.")
		return checkpoint{Version: cursorVersion}
	}

	return cp
}

func openJournal(path string) (*sdjournal.Journal, error) {
	if path == localSystemJournalID {
		j, err := sdjournal.NewJournal()
		if err != nil {
			err = sderr.Wrap(err, "failed to open local journal")
		}
		return j, err
	}

	stat, err := os.Stat(path)
	if err != nil {
		return nil, sderr.Wrap(err, "failed to read meta data for %{path}", path)
	}

	if stat.IsDir() {
		j, err := sdjournal.NewJournalFromDir(path)
		if err != nil {
			err = sderr.Wrap(err, "failed to open journal directory %{path}", path)
		}
		return j, err
	}

	j, err := sdjournal.NewJournalFromFiles(path)
	if err != nil {
		err = sderr.Wrap(err, "failed to open journal file %{path}", path)
	}
	return j, err
}

// seekJournal tries to seek to the last known position in the journal, so we can continue collecting
// from the last known position.
// The checkpoint is ignored if the user has configured the input to always
// seek to the head/tail of the journal on startup.
func seekJournal(log *logp.Logger, reader *reader, cp checkpoint, seek, defaultSeek seekMode) {
	mode := seek
	if mode == seekCursor && cp.Position == "" {
		mode = defaultSeek
		if mode != seekHead && mode != seekTail {
			log.Error("Invalid option for cursor_seek_fallback")
			mode = seekHead
		}
	}

	err := reader.Seek(mode, cp.Position)
	if err != nil {
		log.Error("Continue from current position. Seek failed with: %v", err)
	}
}
