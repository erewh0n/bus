package log

type Group struct {
	id        string
	consumers []*Consumer
	lastIndex int
	stop      bool
}

func NewGroupConsumer(id string) *Group {
	return &Group{
		id:        id,
		consumers: []*Consumer{},
		lastIndex: 0,
		stop:      false,
	}
}
