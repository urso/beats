package cursor

import (
	input "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/beat"
)

type Publisher interface {
	Publish(event beat.Event, cursor interface{}) error
}

type cursorPublisher struct {
	ctx    *input.Context
	client beat.Client
	cursor *Cursor
}

func (c *cursorPublisher) Publish(event beat.Event, cursorUpdate interface{}) error {
	op, err := c.cursor.session.CreateUpdateOp(c.cursor.resource, cursorUpdate)
	if err != nil {
		return err
	}

	event.Private = op
	c.client.Publish(event)
	return c.ctx.Cancelation.Err()
}
