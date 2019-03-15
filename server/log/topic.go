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

type Consumer interface {
	ID() string
	Handle(*Message) error
}

type ConsumerResult struct {
	AckStatus string
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
			Error: err,
		}
	}

	topic.notifyCursors(entry)
	return &ProducerResult{
		Entry: entry,
		Error: nil,
	}
}

func (topic *Topic) Consume(consumerID string) (*Cursor, error) {
	var cursor *Cursor
	cursor, ok := topic.cursors[consumerID]
	if !ok {
		cursorOffsetStore, err := topic.offsetStore.GetCursorOffsetStore(consumerID)
		if err != nil {
			return nil, err
		}
		cursor, err = newCursor(topic.store, cursorOffsetStore)
		if err != nil {
			return nil, err
		}
		topic.cursors[consumerID] = cursor
	}
	return cursor, nil
}

func (topic *Topic) notifyCursors(entry *Entry) {
	for _, cursor := range topic.cursors {
		cursor.notify(entry)
	}
}
