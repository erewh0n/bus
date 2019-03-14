package log

type Cursor struct {
	receiver    chan *Message
	logStore    LogStore
	offsetStore CursorOffsetStore
	offset      []byte
	stop        bool
	signaler    *Signaler
}

func newCursor(logStore LogStore, offsetStore CursorOffsetStore) (*Cursor, error) {
	cursor := &Cursor{
		receiver:    make(chan *Message),
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

func (cursor *Cursor) notify(entry *Entry) {
	cursor.signaler.Notify()
}

func (cursor *Cursor) process() {
	handler := func(offset []byte, value []byte) error {
		message := NewMessage(NewEntry(offset, value), func() {
			cursor.offsetStore.Set(offset)
		})
		cursor.receiver <- message
		return nil
	}

	for !cursor.stop {
		offset, err := cursor.logStore.Next(cursor.offset, handler)
		if err != nil {
			// TODO: if this is DB problem we need a failure state transition
			// but if it's consumer then ... warn? drop consumer?
		}
		cursor.offset = offset
		cursor.signaler.Wait()
	}
}

func (cursor *Cursor) Next() *Message {
	return <-cursor.receiver
}
