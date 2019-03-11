package log

import "sync"

type Message struct {
	ack    *sync.WaitGroup
	Offset []byte
	Value  []byte
}

func NewMessage(entry *Entry) *Message {
	message := &Message{
		ack:    &sync.WaitGroup{},
		Offset: entry.Offset,
		Value:  entry.Value,
	}
	message.ack.Add(1)
	return message
}

func (message *Message) Ack() {
	message.ack.Done()
}

func (message *Message) Wait() {
	message.ack.Wait()
}
