package main

import "errors"

type Message struct {
	Body   string
	Offset string
}

type Log interface {
	Read() (*Message, error)
	Write(*Message) error
	Next(string, func(*Message) error) (string, error)
}

var (
	ErrKeyNotFound = errors.New("That key d")
)

func NewMessage(body string) *Message {
	return &Message{
		Body: body,
	}
}
