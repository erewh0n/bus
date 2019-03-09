package log

import (
	"sync"
)

type Topics struct {
	existing     map[string]*Topic
	topicFactory func(string) (*Topic, error)
	mutex        *sync.Mutex
}

func NewTopics(topicFactory func(string) (*Topic, error)) *Topics {
	existing := map[string]*Topic{}
	return &Topics{
		existing:     existing,
		topicFactory: topicFactory,
		mutex:        &sync.Mutex{},
	}
}

func (topics *Topics) Produce(topicName string, message []byte) *ProduceResult {
	topic, err := topics.findOrCreate(topicName)
	if err != nil {
		return &ProduceResult{
			Entry: nil,
			Error: err,
		}
	}
	return topic.Produce(message)
}

func (topics *Topics) Consume(topicName string, consumerGroupName string, consumer *Consumer) error {
	topic, err := topics.findOrCreate(topicName)
	if err != nil {
		return err
	}
	return topic.Consume(consumerGroupName, consumer)
}

// func (topics *Topics) ConsumeNext(topicName string, consumerGroupName string, consumerID string) (*ConsumerResult, error) {
// 	topic, err := topics.findOrCreate(topicName)
// 	if err != nil {
// 		return err
// 	}

// 	err = topic.Consume(consumerGroupName, NewConsumer(consumerID, func(entry *Entry) *ConsumerResult) {
// 		if
// 	})
// 	return nil, nil
// }

func (topics *Topics) findOrCreate(topicName string) (*Topic, error) {
	topics.mutex.Lock()
	defer topics.mutex.Unlock()
	_, ok := topics.existing[topicName]
	if !ok {
		topic, err := topics.topicFactory(topicName)
		if err != nil {
			return nil, err
		}
		topics.existing[topicName] = topic
	}
	return topics.existing[topicName], nil
}
