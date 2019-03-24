package main

import (
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
	bus, err := NewBus()
	if err != nil {
		logger.Fatalf("Error starting bus", err)
	}

	handler := NewHandler(bus, logger)

	router := httprouter.New()
	router.POST("/topics/:name", handler.PostMessage)
	router.GET("/topics/:name", handler.GetMessage)
	// router.GET("/topics", handler.GetTopics)

	logger.Info("HTTP server started at 127.0.0.1:9000")
	logger.Fatal(http.ListenAndServe("127.0.0.1:9000", router))
}
