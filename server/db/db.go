package db

import (
	"io/ioutil"
	"os"
	"path"
)

type DB struct {
	workdir string
	logs    map[string]*Log
}

func NewDB(workdir string) (*DB, error) {
	db := &DB{
		workdir: workdir,
		logs:    map[string]*Log{},
	}

	if err := os.MkdirAll(workdir, 0700); err != nil {
		return nil, err
	}
	ids, err := readLogIDs(db.workdir)
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		log, err := NewLog(id, path.Join(db.workdir, id))
		if err != nil {
			return nil, err
		}
		db.logs[id] = log
	}

	return db, nil
}

func (db *DB) Log(logID string) (*Log, error) {
	var err error
	var log *Log
	log, ok := db.logs[logID]
	if !ok {
		log, err = NewLog(logID, path.Join(db.workdir, logID))
		if err != nil {
			return nil, err
		}
		db.logs[logID] = log
	}

	return log, nil
}

func readLogIDs(path string) ([]string, error) {
	names := []string{}

	files, err := ioutil.ReadDir(path)
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
