// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package backend

// Registry provides access to stores managed by the backend storage.
type Registry interface {
	// Access opens a store. The store will be closed by the registry, once all
	// accessors have closed the store.
	//
	// The Store instance returned must be threadsafe.
	Access(name string) (Store, error)

	// Close must release any resource held by the backend.
	// Close is only called after all stores have been closed.
	Close() error
}

// Key represents the internal key.
type Key string

// ValueDecoder is used to decode values into go structs or maps within a transaction.
// A ValueDecoder must be invalidated once the owning transaction has been closed.
type ValueDecoder interface {
	Decode(to interface{}) error
}

// Store is used to create a transaction in a target storage (e.g.
// index/table/directory).
//
// A Store instance must be threadsafe.
//
// The store is not required to keep track of accessors. The Frontend keeps
// track of shared ownership and closes the store after all owners have closed
// the store.
type Store interface {
	// Close should close the store and release all used resources.
	Close() error

	// Begin should start a new transaction. Transaction are assumed to be atomic and isolated.
	// Pending writes must not be visible by active/concurrent readers.
	Begin(readonly bool) (Tx, error)
}

// Tx implements the actual access to a store.
// Transactions must be isolated and updates atomic. Transaction must guarantee
// consistent and valid state when re-opening a registry/store after restarts.
//
// The frontend guarantees that Write operations can not be called on a readonly transaction.
type Tx interface {
	// Close is always called at the end of a transaction.
	Close() error

	// Rollback is called at the and of a writable transaction, either by the user
	// or if the transaction has been closed without committing any changes.
	Rollback() error

	// Commit should persist all pending changes. The only valid operation after Commit is Close.
	// The Frontend will forbid any other operation automatically.
	Commit() error

	// Has looks up a key in the registry. If the value is not present false must be returned.
	// The error return must be non-nil, only if an error occured that made the
	// lookup impossible. Key presence must not be reported via error values.
	Has(key Key) (bool, error)

	// Get returns a value decoder if the key is present. The decoder must be `nil` if the key is unknown.
	// The error return must be non-nil, only if an error occured that made the
	// lookup impossible. Key presence must not be reported via error values.
	Get(key Key) (ValueDecoder, error)

	// Set overwrites the value in the Store. The change must only become visible once the transaction
	// has been completed.
	Set(key Key, from interface{}) error

	// Update "merges" the value for said key with the value in the Store. Fields
	// that are not present in `value` must not be changed. The change must only
	// become visible once the transaction has been completed.
	Update(key Key, value interface{}) error

	// Remove removes the key-value pair from the Store. The change must only
	// become visible once the transaction has been completed.
	Remove(Key) error

	// EachKey and Each allow iteration over each known key-value pair in the store.
	EachKey(internal bool, fn func(Key) (bool, error)) error
	Each(internal bool, fn func(Key, ValueDecoder) (bool, error)) error
}
