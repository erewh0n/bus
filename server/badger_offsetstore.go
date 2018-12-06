package main

import (
	"fmt"

	"github.com/dgraph-io/badger"
)

type BadgerOffsetStore struct {
	db *badger.DB
}

func NewBadgerOffsetStore(name string) (*BadgerOffsetStore, error) {
	opts := badger.DefaultOptions
	opts.Dir = fmt.Sprintf("/tmp/badger/offsets/%s", name)
	opts.ValueDir = fmt.Sprintf("/tmp/badger/offsets/%s", name)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &BadgerOffsetStore{
		db,
	}, nil
}

func (store *BadgerOffsetStore) Get(key string) (string, error) {
	offset := []byte("")
	err := store.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		offset, err = item.ValueCopy(nil)
		return err
	})

	return string(offset), err
}

func (store *BadgerOffsetStore) Set(key string, offset string) error {
	return store.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), []byte(offset))
	})
}
