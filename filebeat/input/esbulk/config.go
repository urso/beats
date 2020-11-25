package esbulk

import "github.com/elastic/beats/v7/libbeat/common"

type settings struct {
	Address string
}

func readSettings(cfg *common.Config) (settings, error) {
	settings := defaultSettings()
	err := cfg.Unpack(&settings)
	return settings, err
}

func defaultSettings() settings {
	return settings{
		Address: "localhost:9200",
	}
}
