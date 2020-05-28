package memlog2

type (
	op interface {
		name() string
	}

	opSet struct {
		K string
		V interface{}
	}

	/*
		opUpdate struct {
			K string
			V interface{}
		}
	*/

	opRemove struct {
		K string
	}
)

// operation type names
const (
	opValSet = "Set"
	// opValUpdate = "update"
	opValRemove = "remove"
)

func (*opSet) name() string    { return opValSet }
func (*opRemove) name() string { return opValRemove }

// func (*opUpdate) name() string { return opValUpdate }
