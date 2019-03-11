package groups

import "github.com/izzatbamieh/bus/server/log"

type Group struct {
	id        string
	consumers []log.Consumer
	lastIndex int
	stop      bool
}

type MessageHandler func(*log.Message) error

func newGroup(groupID string, consumerID string) *Group {
	return &Group{
		id:        groupID,
		consumers: []log.Consumer{},
		lastIndex: 0,
		stop:      false,
	}
}

func (group *Group) ID() string {
	return group.id
}

func (group *Group) Handle(message *log.Message) error {
	i := group.lastIndex
	for ; i < len(group.consumers); i++ {
		err := group.consumers[i].Handle(message)
		if err == nil {
			// TODO: handle failures
			break
		}
	}
	group.lastIndex = (group.lastIndex + 1) % len(group.consumers)
	return nil
}

func (group *Group) Join(consumer log.Consumer) {
	// TODO: silently overwrite consumer? cool?
	for i, existing := range group.consumers {
		if existing.ID() == consumer.ID() {
			group.consumers[i] = consumer
			return
		}
	}
	group.consumers = append(group.consumers, consumer)
}
