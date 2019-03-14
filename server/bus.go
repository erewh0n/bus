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

type Group struct {
	index     int
	cursor    *log.Cursor
	receivers []*Receiver
}

func newGroup(cursor *log.Cursor, receiver *Receiver) *Group {
	group := &Group{
		index:     0,
		cursor:    cursor,
		receivers: []*Receiver{},
	}
	group.Join(receiver)
	go group.roundrobin()

	return group
}

func (group *Group) roundrobin() {
	for {
		group.receivers[group.index].message <- group.cursor.Next()
		group.index = (group.index + 1) % len(group.receivers)
	}
}

func (group *Group) Join(receiver *Receiver) {
	for i, existing := range group.receivers {
		if existing.ID == receiver.ID {
			group.receivers[i] = receiver
			return
		}
	}
	group.receivers = append(group.receivers, receiver)
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
