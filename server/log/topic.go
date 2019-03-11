package log

type Topic struct {
	name        string
	store       LogStore
	offsetStore TopicOffsetStore
	cursors     map[string]*Cursor
}

type ProducerResult struct {
	Error error
	Entry *Entry
}

var (
	ACK_OK      = "OK"
	ACK_TIMEOUT = "TIMEOUT"
)

type Consumer interface {
	ID() string
	Handle(*Message) error
}

type ConsumerResult struct {
	AckStatus string
}

type ConsumerHandler struct {
	id      string
	handler func(*Message) error
}

func NewConsumerHandler(id string, handler func(*Message) error) *ConsumerHandler {
	return &ConsumerHandler{
		id,
		handler,
	}
}

func (handler *ConsumerHandler) ID() string {
	return handler.id
}

func (handler *ConsumerHandler) Handle(message *Message) error {
	return handler.handler(message)
}

func NewTopic(name string, store LogStore, offsetStore TopicOffsetStore) *Topic {
	return &Topic{
		name:        name,
		store:       store,
		offsetStore: offsetStore,
		cursors:     map[string]*Cursor{},
	}
}

func (topic *Topic) Produce(value []byte) *ProducerResult {
	entry, err := topic.store.Append(value)
	if err != nil {
		return &ProducerResult{
			Entry: nil,
			Error: err, // TODO: wrap?
		}
	}

	topic.notifyConsumers(entry)
	return &ProducerResult{
		Entry: entry,
		Error: nil,
	}
}

func (topic *Topic) Consume(consumer Consumer) error {
	var cursor *Cursor
	cursor, ok := topic.cursors[consumer.ID()]
	if !ok {
		cursorOffsetStore, err := topic.offsetStore.GetCursorOffsetStore(consumer.ID())
		if err != nil {
			return err
		}
		cursor, err = newCursor(consumer, topic.store, cursorOffsetStore)
		if err != nil {
			return err
		}
		topic.cursors[consumer.ID()] = cursor
	}
	return nil
}

func (topic *Topic) notifyConsumers(entry *Entry) {
	for _, cursor := range topic.cursors {
		cursor.Notify(entry)
	}
}
