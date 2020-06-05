package memlog

import "github.com/elastic/beats/v7/libbeat/common"

type (
	op interface {
		name() string
	}

	opSet struct {
		K string
		V common.MapStr
	}

	opRemove struct {
		K string
	}
)

// operation type names
const (
	opValSet    = "Set"
	opValRemove = "remove"
)

func (*opSet) name() string    { return opValSet }
func (*opRemove) name() string { return opValRemove }
