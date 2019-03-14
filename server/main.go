package main

import (
	"fmt"
	"time"

	"github.com/rs/xid"

	"github.com/izzatbamieh/bus/server/badgerlog"
	"github.com/izzatbamieh/bus/server/log"

	"go.uber.org/zap"
)

func logEntry(logger *zap.SugaredLogger, entry *log.Entry) {
	id, err := xid.FromBytes(entry.Offset)
	if err != nil {
		logger.Fatal("Bad offset value!")
	}
	logger.Infof("%s:%s", id.String(), string(entry.Value))
}

func producer(logger *zap.SugaredLogger, topics *log.Topics, producers *uint32) {
	for {
		result := topics.Produce("test", []byte(fmt.Sprintf("message %d", *producers)))
		if result.Error != nil {
			logger.Error(result.Error)
		}
		// logger.Info("Sent message ", *producers)
		// time.Sleep(500 * time.Millisecond)
		*producers++
	}
}

func consumer(logger *zap.SugaredLogger, bus *Bus, groupID string, consumerID string, consumers *uint32) {
	receiver, err := bus.Receive("test", groupID, consumerID)
	if err != nil {
		logger.Fatal(err)
	}

	for {
		message := receiver.Next()
		message.Ack()
		*consumers++
	}
}

func main() {
	zapLogger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer zapLogger.Sync()
	logger := zapLogger.Sugar()

	logger.Info("Bus starting...")
	topics := log.NewTopics(func(name string) (*log.Topic, error) {
		badgerLog, err := badgerlog.NewBadgerLog(name)
		if err != nil {
			return nil, err
		}
		offsetStore, err := badgerlog.NewBadgerOffsetStore(name)
		if err != nil {
			return nil, err
		}
		return log.NewTopic(name, badgerLog, (log.TopicOffsetStore)(offsetStore)), nil
	})

	bus := NewBus(topics)
	producers := uint32(0)
	consumer1 := uint32(0)
	consumer2 := uint32(0)
	consumer3 := uint32(0)
	go producer(logger, topics, &producers)
	go consumer(logger, bus, "test-1", "1", &consumer1)
	go consumer(logger, bus, "test-1", "2", &consumer2)
	go consumer(logger, bus, "test-1", "3", &consumer3)
	time.Sleep(5 * time.Second)
	logger.Info("Producer count", producers)
	logger.Info("Consumer 1 count", consumer1)
	logger.Info("Consumer 2 count", consumer2)
	logger.Info("Consumer 3 count", consumer3)
	// handler := NewHandler(bus, logger)

	// router := httprouter.New()
	// router.POST("/topics/:name", handler.PostMessage)
	// router.GET("/topics/:name", handler.GetMessage)
	// router.GET("/topics", handler.GetTopics)

	// logger.Info("HTTP server started at 127.0.0.1:9000")
	// logger.Fatal(http.ListenAndServe("127.0.0.1:9000", router))
}
