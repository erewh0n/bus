package db

type Reader struct {
	entryStore        *EntryStore
	cursorOffsetStore *CursorOffsetStore
	stop              bool
	newWrites         *Signaler
	offset            []byte
	distributor       *Distributor
}

func newReader(entryStore *EntryStore, cursorOffsetStore *CursorOffsetStore, newWrites *Signaler) (*Reader, error) {
	offset, err := cursorOffsetStore.Get()
	if err != nil {
		return nil, err
	}
	reader := &Reader{
		entryStore:        entryStore,
		cursorOffsetStore: cursorOffsetStore,
		newWrites:         newWrites,
		stop:              false,
		offset:            offset,
		distributor:       NewRoundRobin(),
	}

	go reader.process()

	return reader, nil
}

func (reader *Reader) Notify(entry *Entry) {
	reader.newWrites.Notify()
}

func (reader *Reader) process() {
	handler := func(offset []byte, value []byte) error {
		message := NewMessage(NewEntry(offset, value), func() {
			reader.cursorOffsetStore.Set(offset)
		})
		reader.distributor.Send(message)

		return nil
	}

	var err error
	for !reader.stop {
		reader.offset, err = reader.entryStore.Stream(reader.offset, handler)
		if err != nil {
			// TODO: if this is DB problem we need a failure state transition
			// but if it's consumer then ... warn? drop consumer?
		}
		reader.newWrites.Wait()
	}
}

func (reader *Reader) Join(id string) *Receiver {
	return reader.distributor.Join(id)
}
