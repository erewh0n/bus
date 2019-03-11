package groups

import "github.com/izzatbamieh/bus/server/log"

type Groups map[string]*Group

func NewGroups() Groups {
	return Groups{}
}

func (groups Groups) NewConsumer(groupID string, consumerID string, handler func(message *log.Message) error) *Group {
	var group *Group
	group, ok := groups[groupID]
	if !ok {
		group = newGroup(groupID, consumerID)
		groups[groupID] = group
	}
	group.Join(log.NewConsumerHandler(consumerID, handler))
	return group
}
