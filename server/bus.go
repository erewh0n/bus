package main

import (
	"github.com/izzatbamieh/bus/server/log"
)

type Receiver struct {
	ID      string
	message chan *log.Message
}

func newReceiver(id string) *Receiver {
	return &Receiver{
		ID:      id,
		message: make(chan *log.Message),
	}
}

func (receiver *Receiver) Next() *log.Message {
	return <-receiver.message
}

type Bus struct {
	Topics *log.Topics
	groups map[string]*Group
}

func NewBus(topics *log.Topics) *Bus {
	return &Bus{
		Topics: topics,
		groups: map[string]*Group{},
	}
}

func (bus *Bus) Produce(topicID string, message []byte) *log.ProducerResult {
	return bus.Topics.Produce(topicID, message)
}

func (bus *Bus) Receive(topicID string, groupID string, cursorID string) (*Receiver, error) {
	topic, err := bus.Topics.GetTopic(topicID)
	if err != nil {
		return nil, err
	}
	cursor, err := topic.Consume(cursorID)
	if err != nil {
		return nil, err
	}

	receiver := newReceiver(cursorID)
	var group *Group
	group, ok := bus.groups[groupID]
	if !ok {
		group = newGroup(cursor, receiver)
		bus.groups[groupID] = group
	}
	group.Join(receiver)

	return receiver, nil
}
