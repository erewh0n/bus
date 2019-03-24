package main

import (
	"github.com/izzatbamieh/bus/server/db"
)

type Bus struct {
	db *db.DB
}

func NewBus() (*Bus, error) {
	db, err := db.NewDB("/tmp/bus")
	if err != nil {
		return nil, err
	}
	return &Bus{
		db,
	}, nil
}

func (bus *Bus) GetReader(logID string, readerID string) (*db.Reader, error) {
	log, err := bus.db.Log(logID)
	if err != nil {
		return nil, err
	}
	return log.Reader(readerID)
}

func (bus *Bus) GetWriter(logID string) (*db.Writer, error) {
	log, err := bus.db.Log(logID)
	if err != nil {
		return nil, err
	}
	return log.Writer(), nil
}
