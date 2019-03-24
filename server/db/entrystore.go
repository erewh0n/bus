package db

import (
	"os"

	"github.com/dgraph-io/badger"
	"github.com/rs/xid"
)

type EntryStore struct {
	id string
	db *badger.DB
}

func NewEntryStore(id string, workdir string) (*EntryStore, error) {
	if err := os.MkdirAll(workdir, 0700); err != nil {
		return nil, err
	}
	opts := badger.DefaultOptions
	opts.Dir = workdir
	opts.ValueDir = workdir
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &EntryStore{
		id,
		db,
	}, nil
}

func (store *EntryStore) ID() string {
	return store.id
}

func (store *EntryStore) Stream(key []byte, receiver func([]byte, []byte) error) ([]byte, error) {
	currentKey := key
	err := store.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		if len(key) > 0 {
			it.Seek(key)
			if it.Valid() {
				it.Next()
			}
		} else {
			it.Rewind()
		}
		for ; it.Valid(); it.Next() {
			item := it.Item()
			err := item.Value(func(value []byte) error {
				return receiver(item.Key(), value)
			})
			if err != nil {
				return err
			}
			currentKey = item.KeyCopy(nil)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return currentKey, nil
}

func (store *EntryStore) Append(value []byte) (*Entry, error) {
	offset := xid.New().Bytes()
	err := store.db.Update(func(txn *badger.Txn) error {
		return txn.Set(offset, value)
	})
	if err != nil {
		return nil, err
	}
	return &Entry{offset, value}, nil
}
