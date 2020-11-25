package grpcbatch

import (
	"time"

	"github.com/elastic/beats/v7/libbeat/common"
)

type settings struct {
	BulkMaxSize int           `config:"bulk_max_size"`
	Timeout     time.Duration `config:"timeout"`
	Backoff     backoff       `config:"backoff"`
}

type backoff struct {
	Init time.Duration
	Max  time.Duration
}

const defaultHost = "http://localhost:9292"

func readSettings(cfg *common.Config) (settings, error) {
	settings := defaultSettings()
	err := cfg.Unpack(&settings)
	return settings, err
}

func defaultSettings() settings {
	return settings{
		BulkMaxSize: 2 * 1048,
		Timeout:     5 * time.Minute,
		Backoff: backoff{
			Init: 1 * time.Second,
			Max:  60 * time.Second,
		},
	}
}
