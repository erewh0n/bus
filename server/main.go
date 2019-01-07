package main

import (
	"fmt"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
)

func main() {
	zapLogger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer zapLogger.Sync()
	logger := zapLogger.Sugar()
	logger.Info("Bus starting...")

	bus := NewBus(NewChannelFactory)
	names, err := ListAll()
	if err != nil {
		panic(err)
	}
	for _, name := range names {
		fmt.Printf("Name: %s\n", name)
	}
	// os.Exit(0)
	handler := NewHandler(bus, logger)

	router := httprouter.New()
	router.POST("/topics/:name", handler.PostMessage)
	router.GET("/topics/:name", handler.GetMessage)
	router.GET("/topics", handler.GetTopics)

	logger.Info("HTTP server started at 127.0.0.1:9000")
	logger.Fatal(http.ListenAndServe("127.0.0.1:9000", router))
}
