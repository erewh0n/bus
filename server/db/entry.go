package db

type Entry struct {
	Offset []byte
	Value  []byte
}

func NewEntry(offset, value []byte) *Entry {
	return &Entry{
		Offset: offset,
		Value:  value,
	}
}
