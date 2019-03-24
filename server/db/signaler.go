package db

import (
	"sync"
)

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
