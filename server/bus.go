package main

type SendResult struct {
	Err error
}

type ReceiveResult struct {
	Message *Message
	Err     error
}

type Bus struct {
	channels       map[string]*Channel
	channelFactory func(name string) (*Channel, error)
}

func NewBus(channelFactory func(name string) (*Channel, error)) *Bus {
	return &Bus{
		channels:       map[string]*Channel{},
		channelFactory: channelFactory,
	}
}

func (bus *Bus) Send(name, message string) *SendResult {
	_, ok := bus.channels[name]
	if !ok {
		channel, err := bus.channelFactory(name)
		if err != nil {
			return &SendResult{
				Err: err,
			}
		}
		bus.channels[name] = channel
	}
	err := bus.channels[name].Send(NewMessage(message))
	return &SendResult{
		Err: err,
	}
}

func (bus *Bus) Receive(channelName, receiverName, clientName string) (chan ReceiveResult, error) {
	_, ok := bus.channels[channelName]
	if !ok {
		channel, err := bus.channelFactory(channelName)
		if err != nil {
			return nil, err
		}
		bus.channels[channelName] = channel
	}
	return bus.channels[channelName].Receive(clientName, receiverName)
}

func (bus *Bus) GetReceiver(name, group string) (*Receiver, error) {
	return bus.channels[name].receivers[group], nil
}
