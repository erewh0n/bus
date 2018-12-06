package main

import (
	"errors"
	"time"
)

type OffsetStore interface {
	Get(string) (string, error)
	Set(string, string) error
}

type Receiver struct {
	id           string
	offset       string
	offsetStore  OffsetStore
	handlers     map[string]chan ReceiveResult
	log          Log
	available    map[string]*Receiver
	hasNewWrites bool
}

type Channel struct {
	name        string
	log         Log
	receivers   map[string]*Receiver
	offsetStore OffsetStore
	available   map[string]*Receiver
}

func NewChannelFactory(name string) (*Channel, error) {
	offsetStore, err := NewBadgerOffsetStore(name)
	if err != nil {
		return nil, err
	}
	log, err := NewBadgerLog(name)
	if err != nil {
		return nil, err
	}
	return NewChannel(name, log, offsetStore), nil
}

func NewChannel(name string, log Log, offsetStore OffsetStore) *Channel {
	return &Channel{
		name:        name,
		log:         log,
		receivers:   map[string]*Receiver{},
		offsetStore: offsetStore,
	}
}

func (channel *Channel) Receive(clientId, receiverId string) (chan ReceiveResult, error) {
	_, ok := channel.receivers[receiverId]
	if !ok {
		receiver, err := NewReceiver(receiverId, channel.offsetStore, channel.log, channel.available)
		if err != nil {
			return nil, err
		}
		channel.receivers[receiverId] = receiver
	}
	handler := make(chan ReceiveResult)
	channel.receivers[receiverId].AddHandler(clientId, handler)

	return handler, nil
}

func (channel *Channel) Send(message *Message) error {
	err := channel.log.Write(message)
	if err != nil {
		return err
	}
	return nil
}

func NewReceiver(id string, offsetStore OffsetStore, log Log, available map[string]*Receiver) (*Receiver, error) {
	offset, err := offsetStore.Get(id)
	if err != nil {
		return nil, err
	}
	receiver := &Receiver{
		id:          id,
		offset:      offset,
		offsetStore: offsetStore,
		handlers:    map[string]chan ReceiveResult{},
		log:         log,
	}
	receiver.process()
	return receiver, nil
}

func (receiver *Receiver) AddHandler(id string, handler chan ReceiveResult) {
	_, ok := receiver.handlers[id]
	if ok {
		close(handler)
	}
	receiver.handlers[id] = handler
}

func (receiver *Receiver) distribute(message *Message) error {
	for _, v := range receiver.handlers {
		v <- ReceiveResult{
			Message: message,
			Err:     nil,
		}
		return nil
	}
	return errors.New("no handlers accepted the message")
}

func (receiver *Receiver) process() {
	go func() {
		for receiver.waitUntilThereIsMoreToRead() {
			receiver.processMessages()
		}
	}()
}

func (receiver *Receiver) waitUntilThereIsMoreToRead() bool {
	// TODO: not scalable or performant
	time.Sleep(10 * time.Millisecond)
	return true
}

func (receiver *Receiver) continueProcessing() bool {
	if receiver.hasNewWrites {
		receiver.hasNewWrites = false
		return true
	}
	return false
}

func (receiver *Receiver) processMessages() {
	next, err := receiver.log.Next(receiver.offset, receiver.distribute)
	if err != nil {
		// TODO
		panic(err)
	} else {
		err = receiver.offsetStore.Set(receiver.id, next)
		if err != nil {
			// TODO
			panic(err)
		} else {
			receiver.offset = next
		}
	}
}
