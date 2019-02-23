package main

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

type CRDBLog struct {
	name   string
	db     *sql.DB
	reader func(string) (*Message, error)
}

func createReader(stmt *sql.Stmt) func(string) (*Message, error) {
	return func(offset string) (*Message, error) {
		row := stmt.QueryRow(offset)
		data := ""
		row.Scan(data)
		return &Message{
			Body:   data,
			Offset: offset,
		}, nil
	}
}

func NewCRDBLog(name string) (*CRDBLog, error) {
	log := &CRDBLog{
		name: name,
	}
	db, err := sql.Open("postgres", "user=test password=test dbname=test")
	if err != nil {
		return nil, err
	}
	log.db = db
	stmt, err := log.db.Prepare(fmt.Sprintf("select top 1 * from %s where idx>?", name))
	if err != nil {
		return nil, err
	}
	log.reader = createReader(stmt)
	return log, nil
}

func (log *CRDBLog) Read() (*Message, error) {
	message, err := log.reader("foo")
	if err != nil {
		return nil, err
	}
	return message, nil
}

func (log *CRDBLog) Write(message *Message) error {
	tx, err := log.db.Begin()
	if err != nil {
		return err
	}
	_, err = tx.Exec(fmt.Sprintf("insert into %s values (%s)", log.name, message.Body))
	if err != nil {
		return err
	}
	return nil
}

func (log *CRDBLog) Next(string, func(*Message) error) (string, error) {
	return "", nil
}
