package db

import (
	"path"
)

type Log struct {
	id        string
	workdir   string
	length    uint64
	entries   *EntryStore
	offsets   *OffsetStore
	newWrites *Signaler
}

func NewLog(id string, workdir string) (*Log, error) {
	entries, err := NewEntryStore(id, workdir)
	if err != nil {
		return nil, err
	}
	offsets, err := NewOffsetStore(id, path.Join(workdir, "offsets"))
	if err != nil {
		return nil, err
	}

	newWrites := NewSignaler(false)
	length := uint64(0)
	return &Log{
		id,
		workdir,
		length,
		entries,
		offsets,
		newWrites,
	}, nil
}

func (log *Log) Length() uint64 {
	return log.length
}

func (log *Log) Reader(readerID string) (*Reader, error) {
	cursorOffsetStore, err := log.offsets.GetCursorOffsetStore(readerID)
	if err != nil {
		return nil, err
	}
	return newReader(log.entries, cursorOffsetStore, log.newWrites)
}

func (log *Log) Writer() *Writer {
	return newWriter(func(value []byte) (*Entry, error) {
		entry, err := log.entries.Append(value)
		if err != nil {
			return nil, err
		}
		log.newWrites.Notify()
		return entry, nil
	})
}

type Writer struct {
	append func([]byte) (*Entry, error)
}

func newWriter(append func([]byte) (*Entry, error)) *Writer {
	return &Writer{
		append,
	}
}

func (writer *Writer) Append(value []byte) (*Entry, error) {
	return writer.append(value)
}
