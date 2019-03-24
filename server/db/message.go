package db

import (
	"time"
)

type Message struct {
	ack     *Signaler
	onAckOK func()
	Offset  []byte
	Value   []byte
}

func NewMessage(entry *Entry, onAckOK func()) *Message {
	message := &Message{
		ack:     NewSignaler(false),
		onAckOK: onAckOK,
		Offset:  entry.Offset,
		Value:   entry.Value,
	}
	message.setAckTimeout()

	return message
}

func (message *Message) setAckTimeout() {
	time.AfterFunc(30*time.Second, func() {
		message.ack.Notify()
	})
}

func (message *Message) AckError() {
	message.ack.Notify()
}

func (message *Message) AckOK() {
	message.onAckOK()
	message.ack.Notify()
}

func (message *Message) Wait() {
	message.ack.Wait()
}
