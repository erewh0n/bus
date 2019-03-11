package log

import (
	"errors"
)

type Cursor struct {
	consumer    Consumer
	logStore    LogStore
	offsetStore CursorOffsetStore
	offset      []byte
	stop        bool
	signaler    *Signaler
}

func newCursor(consumer Consumer, logStore LogStore, offsetStore CursorOffsetStore) (*Cursor, error) {
	cursor := &Cursor{
		consumer:    consumer,
		logStore:    logStore,
		offsetStore: offsetStore,
		signaler:    NewSignaler(false),
		stop:        false,
	}

	offset, err := cursor.offsetStore.Get()
	if err != nil {
		return nil, err
	}
	cursor.offset = offset

	cursor.signaler = NewSignaler(false)

	go cursor.process()

	return cursor, nil
}

func (cursor *Cursor) Notify(entry *Entry) {
	cursor.signaler.Notify()
}

func (cursor *Cursor) process() {
	for !cursor.stop {
		offset, err := cursor.logStore.Next(cursor.offset, func(offset []byte, value []byte) error {
			message := NewMessage(NewEntry(offset, value))
			if err := cursor.consumer.Handle(message); err != nil {
				return errors.New("consumer failed to handle message")
			}
			message.Wait()
			return cursor.offsetStore.Set(offset)
		})
		if err != nil {
			// TODO: if this is DB problem we need a failure state transition
			// but if it's consumer then ... warn? drop consumer?
		}
		cursor.offset = offset
		cursor.signaler.Wait()
	}
}
