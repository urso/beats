package cursor

type Cursor struct {
	store    *store
	resource *resource
}

func makeCursor(store *store, res *resource) Cursor {
	return Cursor{store: store, resource: res}
}

func (c Cursor) IsNew() bool { return c.resource.IsNew() }

func (c Cursor) Unpack(to interface{}) error {
	if c.IsNew() {
		return nil
	}
	return c.resource.UnpackCursor(to)
}
