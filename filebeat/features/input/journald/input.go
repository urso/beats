package journald

import (
	"os"
	"time"

	"github.com/coreos/go-systemd/sdjournal"
	input "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/journalbeat/reader"
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
	Version  int
	Position string
}

func Plugin(log *logp.Logger, reg *registry.Registry, defaultStore string) input.Plugin {
	return input.Plugin{
		Name:       "journald",
		Stability:  feature.Beta,
		Deprecated: false,
		Info:       "journald input",
		Doc:        "The journald input collects logs from the local journald service",
		Manager: &CursorInputManager{
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
		paths = []string{reader.LocalSystemJournalID}
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
	j, err := openJournal(src.Name())
	if err != nil {
		return err
	}
	defer j.Close()

	if err := applyMatchers(j, inp.Matches); err != nil {
		return sderr.Wrap(err, "failed to apply filters to the %{path} journal", src.Name())
	}

	log := ctx.Logger.With("path", src.Name())
	checkpoint := initCheckpoint(log, cursor)
	backoff := backoff.NewExpBackoff(ctx.Cancelation.Done(), inp.Backoff, inp.MaxBackoff)

	seekJournal(log, j, checkpoint, inp.Seek, inp.CursorSeekFallback)

	panic("TODO")
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
	if path == reader.LocalSystemJournalID {
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
func seekJournal(log *logp.Logger, j *sdjournal.Journal, cp checkpoint, seek, defaultSeek seekMode) {
	switch seek {
	case seekCursor:
		if cp.Position == "" {
			seekJournalNew(log, j, defaultSeek)
		} else {
			j.SeekCursor(cp.Position)
			_, err := j.Next()
			if err != nil {
				log.Errorf("Error while seeking to cursor: %+v", err)
			} else {
				log.Debug("Seeked to position defined in cursor")
			}
		}
	case seekTail:
		j.SeekTail()
		j.Next()
		log.Debug("Tailing the journal file")
	case seekHead:
		j.SeekHead()
		log.Debug("Reading from the beginning of the journal file")
	default:
		log.Error("Invalid seeking mode")
	}
}

// seekJournalNew is used to seek to a configured journald location, in case we
// do not have store a cursor in the registry file.
func seekJournalNew(log *logp.Logger, j *sdjournal.Journal, mode seekMode) {
	switch mode {
	case seekHead:
		j.SeekHead()
		log.Debug("Seeking method set to cursor, but no state is saved for reader. Starting to read from the beginning")
	case seekTail:
		j.SeekTail()
		j.Next()
		log.Debug("Seeking method set to cursor, but no state is saved for reader. Starting to read from the end")
	default:
		log.Error("Invalid option for cursor_seek_fallback")
	}
}
