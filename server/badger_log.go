package main

import (
	"fmt"
	"io/ioutil"

	"github.com/rs/xid"

	"github.com/dgraph-io/badger"
)

type BadgerLog struct {
	name string
	db   *badger.DB
}

func NewBadgerLog(name string) (Log, error) {
	opts := badger.DefaultOptions
	opts.Dir = fmt.Sprintf("/tmp/badger/logs/%s", name)
	opts.ValueDir = fmt.Sprintf("/tmp/badger/logs/%s", name)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &BadgerLog{
		name,
		db,
	}, nil
}

func ListAll() ([]string, error) {
	names := []string{}
	files, err := ioutil.ReadDir("/tmp/badger/logs")
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		if file.IsDir() {
			names = append(names, file.Name())
		}
	}
	return names, nil
}

func (log *BadgerLog) Read() (*Message, error) {
	var result []byte
	err := log.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(log.name))
		if err != nil {
			return err
		}
		result, err = item.ValueCopy(nil)
		return err
	})
	if err == badger.ErrKeyNotFound {
		return NewMessage(""), ErrKeyNotFound
	}
	if err != nil {
		return NewMessage(""), err
	}
	return NewMessage(string(result)), nil
}

func (log *BadgerLog) Next(key string, receiver func(message *Message) error) (string, error) {
	newKey := key
	err := log.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		if key == "" {
			it.Rewind()
		} else {
			it.Seek([]byte(key))
			if it.Valid() {
				it.Next()
			}
		}
		for ; it.Valid(); it.Next() {
			item := it.Item()
			err := item.Value(func(value []byte) error {
				return receiver(NewMessage(string(value)))
			})
			if err != nil {
				return err
			}
			newKey = string(item.KeyCopy(nil))
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return newKey, nil
}

func (log *BadgerLog) Write(message *Message) error {
	return log.db.Update(func(txn *badger.Txn) error {
		id := xid.New()
		err := txn.Set([]byte(id.String()), []byte(message.Body))
		return err
	})
}
