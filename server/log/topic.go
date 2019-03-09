package log

import (
	"errors"
	"sync"
)

type Topic struct {
	name        string
	store       LogStore
	groups      map[string]*Group
	offsetStore TopicOffsetStore
}

type ProduceResult struct {
	Error error
	Entry *Entry
}

var (
	ACK_OK      = "OK"
	ACK_TIMEOUT = "TIMEOUT"
)

type ConsumerResult struct {
	AckStatus string
}

func NewTopic(name string, store LogStore, offsetStore TopicOffsetStore) *Topic {
	return &Topic{
		name:        name,
		store:       store,
		offsetStore: offsetStore,
		groups:      map[string]*Group{},
	}
}

func (topic *Topic) Produce(value []byte) *ProduceResult {
	entry, err := topic.store.Append(value)
	if err != nil {
		return &ProduceResult{
			Entry: nil,
			Error: err, // TODO: wrap?
		}
	}
	topic.notifyGroups()
	return &ProduceResult{
		Entry: entry,
		Error: nil,
	}
}

func (topic *Topic) Consume(consumerGroupName string, consumer *Consumer) error {
	_, ok := topic.groups[consumerGroupName]
	if !ok {
		offsetStore, err := topic.offsetStore.GetCursorOffsetStore(consumerGroupName)
		if err != nil {
			return err
		}
		consumerGroup, err := newConsumerGroup(consumerGroupName, offsetStore, topic.store)
		if err != nil {
			return err
		}
		topic.groups[consumerGroupName] = consumerGroup
	}
	topic.groups[consumerGroupName].Join(consumer)
	return nil
}

func (topic *Topic) notifyGroups() {
	for _, group := range topic.groups {
		group.Notify()
	}
}

type Signaler struct {
	on   bool
	wait *sync.WaitGroup
}

func NewSignaler(on bool) *Signaler {
	wait := &sync.WaitGroup{}
	if on == false {
		wait.Add(1)
	}
	return &Signaler{
		on,
		wait,
	}
}

func (signaler *Signaler) Wait() {
	if signaler.on == true {
		signaler.wait.Add(1)
		signaler.on = false
	} else {
		signaler.wait.Wait()
		signaler.on = true
	}
}

func (signaler *Signaler) Notify() {
	if signaler.on == false {
		signaler.wait.Done()
		signaler.on = true
	}
}

type Cursor struct {
	ack   *sync.WaitGroup
	Entry *Entry
}

func NewCursor(entry *Entry) *Cursor {
	cursor := &Cursor{
		ack:   &sync.WaitGroup{},
		Entry: entry,
	}
	cursor.ack.Add(1)
	return cursor
}

func (cursor *Cursor) Ack() {
	cursor.ack.Done()
}

type Consumer struct {
	ID     string
	Handle func(*Cursor) error
}

func NewConsumer(id string, handle func(*Cursor) error) *Consumer {
	return &Consumer{
		ID:     id,
		Handle: handle,
	}
}

type Group struct {
	name         string
	offsetStore  CursorOffsetStore
	logStore     LogStore
	offset       []byte
	consumers    []*Consumer
	lastConsumer int
	stop         bool
	signaler     *Signaler
}

func newConsumerGroup(consumerGroupName string, cursorOffsetStore CursorOffsetStore, logStore LogStore) (*Group, error) {
	group := &Group{
		name:         consumerGroupName,
		offsetStore:  cursorOffsetStore,
		logStore:     logStore,
		offset:       []byte{},
		consumers:    []*Consumer{},
		lastConsumer: 0,
		stop:         false,
		signaler:     NewSignaler(false),
	}
	offset, err := group.offsetStore.Get()
	if err != nil {
		return nil, err
	}
	group.offset = offset
	go group.processEntries()
	return group, nil
}

func (group *Group) processEntries() {
	for !group.stop {
		offset, err := group.logStore.Next(group.offset, func(offset []byte, value []byte) error {
			if err := group.consume(NewEntry(offset, value)); err != nil {
				return errors.New("consumer failed to handle message")
			}
			return group.offsetStore.Set(offset)
		})
		if err != nil {
			// TODO: if this is DB problem we need a failure state transition
			// but if it's consumer then ... warn? drop consumer?
		}
		group.offset = offset
		group.signaler.Wait()
	}
}

func (group *Group) consume(entry *Entry) *ConsumerResult {
	i := group.lastConsumer
	for ; i < len(group.consumers); i++ {
		cursor := NewCursor(entry)
		err := group.consumers[i].Handle(cursor)
		if err == nil {
			break
		}
		// TODO: handle single and complete failure
	}
	group.lastConsumer = (group.lastConsumer + 1) % len(group.consumers)
	return &ConsumerResult{
		AckStatus: ACK_OK,
	}
}

func (group *Group) Notify() {
	group.signaler.Notify()
}

func (group *Group) Stop() {
	group.stop = true
	group.signaler.Notify()
}

func (group *Group) Join(consumer *Consumer) {
	// TODO: silently overwrite consumer? cool?
	for i, existing := range group.consumers {
		if existing.ID == consumer.ID {
			group.consumers[i] = consumer
			return
		}
	}
	group.consumers = append(group.consumers, consumer)
}
