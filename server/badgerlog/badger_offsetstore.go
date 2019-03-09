package badgerlog

import (
	"os"
	"path"

	"github.com/dgraph-io/badger"
	"github.com/izzatbamieh/bus/server/log"
)

// type Config struct {
// 	Path string
// }

// type BadgerLogOption func(*Config) error
// type BadgerLogOptions []BadgerLogOption

// func NewBadgerLogOptions() BadgerLogOptions {
// 	return []BadgerLogOption{}
// }

type BadgerOffsetStore struct {
	db *badger.DB
}

func NewBadgerOffsetStore(name string) (*BadgerOffsetStore, error) {
	dbPath := path.Join("/tmp/badger-offsets", name)
	os.MkdirAll(dbPath, 0700)

	opts := badger.DefaultOptions
	opts.Dir = dbPath
	opts.ValueDir = dbPath
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &BadgerOffsetStore{
		db,
	}, nil
}

type BadgerCursorOffsetStore struct {
	name  string
	store *BadgerOffsetStore
}

func NewBadgerCursorOffsetStore(name string, store *BadgerOffsetStore) *BadgerCursorOffsetStore {
	return &BadgerCursorOffsetStore{
		name,
		store,
	}
}

func (cursorOffsetStore *BadgerCursorOffsetStore) Get() ([]byte, error) {
	return cursorOffsetStore.store.Get([]byte(cursorOffsetStore.name))
}

func (cursorOffsetStore *BadgerCursorOffsetStore) Set(offset []byte) error {
	return cursorOffsetStore.store.Set([]byte(cursorOffsetStore.name), offset)
}

// TODO: Signature of the interface for the topic store returns an interface type.
func (store *BadgerOffsetStore) GetCursorOffsetStore(name string) (log.CursorOffsetStore, error) {
	return NewBadgerCursorOffsetStore(name, store), nil
}

func (store *BadgerOffsetStore) Get(key []byte) ([]byte, error) {
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

func (store *BadgerOffsetStore) Set(offset, value []byte) error {
	return store.db.Update(func(txn *badger.Txn) error {
		return txn.Set(offset, value)
	})
}
