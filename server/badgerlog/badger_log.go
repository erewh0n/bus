package badgerlog

import (
	"os"
	"path"

	"github.com/dgraph-io/badger"
	buslog "github.com/izzatbamieh/bus/server/log"
	"github.com/rs/xid"
)

type BadgerLog struct {
	name string
	db   *badger.DB
}

type Config struct {
	Path string
}

type BadgerLogOption func(*Config) error
type BadgerLogOptions []BadgerLogOption

func NewBadgerLogOptions() BadgerLogOptions {
	return []BadgerLogOption{}
}

func (options BadgerLogOptions) Path(path string) BadgerLogOptions {
	return append(options, func(c *Config) error {
		c.Path = path
		return nil
	})
}

func NewBadgerLog(topicName string, options ...BadgerLogOption) (*BadgerLog, error) {
	config := &Config{
		Path: "/tmp/badger-logs",
	}
	for _, option := range options {
		if err := option(config); err != nil {
			return nil, err
		}
	}
	opts := badger.DefaultOptions
	dbPath := path.Join(config.Path, topicName)
	os.MkdirAll(dbPath, 0700)
	opts.Dir = dbPath
	opts.ValueDir = dbPath
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &BadgerLog{
		topicName,
		db,
	}, nil
}

// func ListAll() ([]string, error) {
// 	names := []string{}
// 	files, err := ioutil.ReadDir("/tmp/badger/logs")
// 	if err != nil {
// 		return nil, err
// 	}
// 	for _, file := range files {
// 		if file.IsDir() {
// 			names = append(names, file.Name())
// 		}
// 	}
// 	return names, nil
// }

// func (log *BadgerLog) Read() (*Message, error) {
// 	var result []byte
// 	err := log.db.View(func(txn *badger.Txn) error {
// 		item, err := txn.Get([]byte(log.name))
// 		if err != nil {
// 			return err
// 		}
// 		result, err = item.ValueCopy(nil)
// 		return err
// 	})
// 	if err == badger.ErrKeyNotFound {
// 		return NewMessage(""), ErrKeyNotFound
// 	}
// 	if err != nil {
// 		return NewMessage(""), err
// 	}
// 	return NewMessage(string(result)), nil
// }

func (log *BadgerLog) Next(key []byte, receiver func([]byte, []byte) error) ([]byte, error) {
	currentKey := key
	err := log.db.View(func(txn *badger.Txn) error {
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

func (log *BadgerLog) Append(value []byte) (*buslog.Entry, error) {
	offset := xid.New().Bytes()
	err := log.db.Update(func(txn *badger.Txn) error {
		return txn.Set(offset, value)
	})
	if err != nil {
		return nil, err
	}
	return buslog.NewEntry(offset, value), nil
}
