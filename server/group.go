package main

import "github.com/izzatbamieh/bus/server/log"

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
