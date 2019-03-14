package log

import "sync"

type Message struct {
	ack    *sync.WaitGroup
	onAck  func()
	Offset []byte
	Value  []byte
}

func NewMessage(entry *Entry, onAck func()) *Message {
	message := &Message{
		ack:    &sync.WaitGroup{},
		onAck:  onAck,
		Offset: entry.Offset,
		Value:  entry.Value,
	}
	message.ack.Add(1)
	return message
}

func (message *Message) Ack() {
	message.onAck()
	message.ack.Done()
}

func (message *Message) Wait() {
	message.ack.Wait()
}
