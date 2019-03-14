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

func (topics *Topics) Produce(topicID string, message []byte) *ProducerResult {
	topic, err := topics.findOrCreate(topicID)
	if err != nil {
		return &ProducerResult{
			Entry: nil,
			Error: err,
		}
	}
	return topic.Produce(message)
}

func (topics *Topics) Consume(topicID string, consumerID string) (*Cursor, error) {
	topic, err := topics.findOrCreate(topicID)
	if err != nil {
		return nil, err
	}
	return topic.Consume(consumerID)
}

func (topics *Topics) GetTopic(topicID string) (*Topic, error) {
	return topics.findOrCreate(topicID)
}

func (topics *Topics) findOrCreate(topicID string) (*Topic, error) {
	topics.mutex.Lock()
	defer topics.mutex.Unlock()
	_, ok := topics.existing[topicID]
	if !ok {
		topic, err := topics.topicFactory(topicID)
		if err != nil {
			return nil, err
		}
		topics.existing[topicID] = topic
	}
	return topics.existing[topicID], nil
}
