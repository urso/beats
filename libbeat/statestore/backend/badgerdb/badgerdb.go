package badgerdb

import (
	"bytes"
	"path/filepath"

	"github.com/dgraph-io/badger"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/statestore/backend"
	"github.com/elastic/go-structform/cborl"
	"github.com/elastic/go-structform/gotype"
)

type accessor struct {
	log  *logp.Logger
	home string
}

type store struct {
	db *badger.DB
}

type loggerAdapter logp.Logger

type itemDecoder badger.Item

func New(log *logp.Logger, home string) backend.Registry {
	return &accessor{log: log, home: home}
}

func (*accessor) Close() error { return nil }

func (a *accessor) Access(name string) (backend.Store, error) {
	logger := a.log.With("store", name)

	path := filepath.Join(a.home, name)
	db, err := badger.Open(badger.DefaultOptions(path).WithLogger((*loggerAdapter)(logger)))
	if err != nil {
		return nil, err
	}

	return &store{db: db}, nil
}

func (s *store) Close() error {
	err := s.db.Close()
	s.db = nil
	return err
}

func (s *store) Has(key string) (bool, error) {
	has := false
	err := s.db.View(func(tx *badger.Txn) error {
		item, err := tx.Get([]byte(key))
		if err == badger.ErrKeyNotFound {
			return nil
		} else if err != nil {
			return err
		}

		has = !item.IsDeletedOrExpired()
		return nil
	})
	return has, err
}

func (s *store) Get(key string, into interface{}) error {
	return s.db.View(func(tx *badger.Txn) error {
		item, err := tx.Get([]byte(key))
		if err != nil {
			return err
		}

		dec := (*itemDecoder)(item)
		return dec.Decode(into)
	})
}

func (s *store) Set(key string, from interface{}) error {
	var buf bytes.Buffer
	if err := gotype.Fold(from, cborl.NewVisitor(&buf)); err != nil {
		return err
	}

	return s.db.Update(func(tx *badger.Txn) error {
		return tx.Set([]byte(key), buf.Bytes())
	})
}

func (s *store) Remove(key string) error {
	return s.db.Update(func(tx *badger.Txn) error {
		err := tx.Delete([]byte(key))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
	})
}

func (s *store) Each(fn func(string, backend.ValueDecoder) (bool, error)) error {
	return s.db.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for ; it.Valid(); it.Next() {
			item := it.Item()
			cont, err := fn(string(item.Key()), (*itemDecoder)(item))
			if !cont || err != nil {
				return err
			}
		}

		return nil
	})
}

func (l *loggerAdapter) Debugf(msg string, args ...interface{}) {
	(*logp.Logger)(l).Debugf(msg, args...)
}

func (l *loggerAdapter) Errorf(msg string, args ...interface{}) {
	(*logp.Logger)(l).Errorf(msg, args...)
}

func (l *loggerAdapter) Infof(msg string, args ...interface{}) {
	(*logp.Logger)(l).Infof(msg, args...)
}

func (l *loggerAdapter) Warningf(msg string, args ...interface{}) {
	(*logp.Logger)(l).Warnf(msg, args...)
}

func (dec *itemDecoder) Decode(to interface{}) error {
	item := (*badger.Item)(dec)
	visitor, err := gotype.NewUnfolder(to)
	if err != nil {
		return err
	}

	return item.Value(func(raw []byte) error {
		return cborl.Parse(raw, visitor)
	})
}
