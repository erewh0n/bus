package db

import (
	"os"

	"github.com/dgraph-io/badger"
)

type OffsetStore struct {
	id string
	db *badger.DB
}

func NewOffsetStore(id string, workdir string) (*OffsetStore, error) {
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
	return &OffsetStore{
		id,
		db,
	}, nil
}

type CursorOffsetStore struct {
	name  string
	store *OffsetStore
}

// TODO: Signature of the interface for the topic store returns an interface type.
func (store *OffsetStore) GetCursorOffsetStore(name string) (*CursorOffsetStore, error) {
	return NewCursorOffsetStore(name, store), nil
}

func (store *OffsetStore) Get(key []byte) ([]byte, error) {
	offset := []byte{}
	err := store.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		offset, err = item.ValueCopy(nil)
		return err
	})

	return offset, err
}

func (store *OffsetStore) Set(offset, value []byte) error {
	return store.db.Update(func(txn *badger.Txn) error {
		return txn.Set(offset, value)
	})
}

func NewCursorOffsetStore(name string, store *OffsetStore) *CursorOffsetStore {
	return &CursorOffsetStore{
		name,
		store,
	}
}

func (cursorOffsetStore *CursorOffsetStore) Get() ([]byte, error) {
	return cursorOffsetStore.store.Get([]byte(cursorOffsetStore.name))
}

func (cursorOffsetStore *CursorOffsetStore) Set(offset []byte) error {
	return cursorOffsetStore.store.Set([]byte(cursorOffsetStore.name), offset)
}
