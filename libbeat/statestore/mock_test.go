package statestore

import (
	"github.com/elastic/beats/v7/libbeat/statestore/backend"
	"github.com/stretchr/testify/mock"
)

type mockRegistry struct {
	mock.Mock
}

type mockStore struct {
	mock.Mock
}

func newMockRegistry() *mockRegistry { return &mockRegistry{} }

func (m *mockRegistry) OnAccess(name string) *mock.Call { return m.On("Access", name) }
func (m *mockRegistry) Access(name string) (backend.Store, error) {
	args := m.Called(name)

	var store backend.Store
	if ifc := args.Get(0); ifc != nil {
		store = ifc.(backend.Store)
	}

	return store, args.Error(1)
}

func (m *mockRegistry) OnClose() *mock.Call { return m.On("Close") }
func (m *mockRegistry) Close() error {
	args := m.Called()
	return args.Error(0)
}

func newMockStore() *mockStore { return &mockStore{} }

func (m *mockStore) OnClose() *mock.Call { return m.On("Close") }
func (m *mockStore) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockStore) OnHas(key string) *mock.Call { return m.On("Has", key) }
func (m *mockStore) Has(key string) (bool, error) {
	args := m.Called(key)
	return args.Bool(0), args.Error(1)
}

func (m *mockStore) OnGet(key string) *mock.Call { return m.On("Get", key) }
func (m *mockStore) Get(key string, into interface{}) error {
	args := m.Called(key)
	return args.Error(0)
}

func (m *mockStore) OnRemove(key string) *mock.Call { return m.On("Remove", key) }
func (m *mockStore) Remove(key string) error {
	args := m.Called(key)
	return args.Error(0)
}

func (m *mockStore) OnSet(key string) *mock.Call { return m.On("Set", key) }
func (m *mockStore) Set(key string, from interface{}) error {
	args := m.Called(key)
	return args.Error(0)
}

func (m *mockStore) Each(fn func(string, backend.ValueDecoder) (bool, error)) error {
	args := m.Called(fn)
	return args.Error(0)
}
