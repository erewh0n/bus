package log

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

type LogStore interface {
	// Read() (*Entry, error)
	Append([]byte) (*Entry, error)
	Next([]byte, func([]byte, []byte) error) ([]byte, error)
}

type TopicOffsetStore interface {
	GetCursorOffsetStore(string) (CursorOffsetStore, error)
	Get([]byte) ([]byte, error)
	Set([]byte, []byte) error
}

type CursorOffsetStore interface {
	Get() ([]byte, error)
	Set([]byte) error
}

// var (
// 	ErrKeyNotFound = errors.New("Key not found")
// )
