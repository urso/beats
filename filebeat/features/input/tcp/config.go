package tcp

import (
	"time"

	"github.com/dustin/go-humanize"
	"github.com/elastic/beats/v7/filebeat/harvester"
	"github.com/elastic/beats/v7/filebeat/inputsource/tcp"
)

type config struct {
	tcp.Config                `config:",inline"`
	harvester.ForwarderConfig `config:",inline"`
	LineDelimiter             string `config:"line_delimiter" validate:"nonzero"`
}

func defaultConfig() config {
	return config{
		ForwarderConfig: harvester.ForwarderConfig{
			Type: "tcp",
		},
		Config: tcp.Config{
			Timeout:        time.Minute * 5,
			MaxMessageSize: 20 * humanize.MiByte,
		},
		LineDelimiter: "\n",
	}
}
