package main

type CRDBOffsets struct {
}

func NewCRDBOffsets(name string) (*CRDBOffsets, error) {
	return &CRDBOffsets{}, nil
}

func (offsets *CRDBOffsets) Get(string) (string, error) {
	return "", nil
}

func (offsets *CRDBOffsets) Set(string, string) error {
	return nil
}
