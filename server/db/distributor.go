package db

type Receiver struct {
	ID      string
	message chan *Message
}

func NewReceiver(id string) *Receiver {
	return &Receiver{
		ID:      id,
		message: make(chan *Message),
	}
}

func (receiver *Receiver) Next() *Message {
	return <-receiver.message
}

type Distributor struct {
	index     int
	receivers []*Receiver
}

func NewRoundRobin() *Distributor {
	distributor := &Distributor{
		index: 0,
	}

	return distributor
}

func (distributor *Distributor) Send(message *Message) {
	distributor.receivers[distributor.index].message <- message
	distributor.index = (distributor.index + 1) % len(distributor.receivers)
	message.Wait()
}

func (distributor *Distributor) Join(id string) *Receiver {
	receiver := NewReceiver(id)
	for i, existing := range distributor.receivers {
		if existing.ID == receiver.ID {
			distributor.receivers[i] = receiver
			return receiver
		}
	}
	distributor.receivers = append(distributor.receivers, receiver)
	return receiver
}
