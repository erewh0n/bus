package main

import (
	"net/http"
	"sync"

	jsoniter "github.com/json-iterator/go"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"

	messagelog "github.com/izzatbamieh/bus/server/log"
)

type Runnable interface {
	Run(interface{}, error)
}

type HttpProblem struct {
	Message string
}

type WriteCommand struct {
	Message string
}

type MessageView struct {
	Message string
}

type TopicsView struct {
	Topics []string
}

type Handler struct {
	topics *messagelog.Topics
	log    *zap.SugaredLogger
	json   jsoniter.API
}

func NewHandler(topics *messagelog.Topics, log *zap.SugaredLogger) *Handler {
	return &Handler{
		topics: topics,
		log:    log,
		json:   jsoniter.ConfigCompatibleWithStandardLibrary,
	}
}

func NewMessageView(entry *messagelog.Entry) *MessageView {
	if entry == nil {
		return &MessageView{}
	}
	return &MessageView{
		Message: string(entry.Value),
	}
}

func (handler *Handler) PostMessage(response http.ResponseWriter, request *http.Request, params httprouter.Params) {
	writeCommand := &WriteCommand{}
	err := handler.json.NewDecoder(request.Body).Decode(writeCommand)
	if err != nil {
		response.WriteHeader(400)
		handler.json.NewEncoder(response).Encode(HttpProblem{
			Message: "Could not understand the request",
		})
		return
	}

	result := handler.topics.Produce(params.ByName("name"), []byte(writeCommand.Message))
	if result.Error != nil {
		handler.log.Error(err)
		response.WriteHeader(500)
		return
	}
	response.WriteHeader(204)
}

func (handler *Handler) GetMessage(response http.ResponseWriter, request *http.Request, params httprouter.Params) {
	name := params.ByName("name")
	queries := request.URL.Query()
	group := queries.Get("group")
	if group == "" {
		response.WriteHeader(400)
		handler.json.NewEncoder(response).Encode(HttpProblem{
			Message: "Must supply non-empty 'group' query parameter",
		})
		return
	}
	clientID := queries.Get("client_id")
	if clientID == "" {
		response.WriteHeader(400)
		handler.json.NewEncoder(response).Encode(HttpProblem{
			Message: "Must supply non-empty 'client_id' query parameter",
		})
		return
	}

	var outerEntry *messagelog.Entry
	reply := &sync.WaitGroup{}
	reply.Add(1)
	ack := &sync.WaitGroup{}
	ack.Add(1)
	mh := func(entry *messagelog.Entry) *messagelog.ConsumerResult {
		outerEntry = entry
		reply.Done()
		ack.Wait()
		return &messagelog.ConsumerResult{}
	}
	result := handler.topics.Consume(name, group, messagelog.NewConsumer(clientID, mh))
	if result.Error != nil {
		response.WriteHeader(500)
		return
	}
	reply.Wait()

	if result.Err != nil {
		response.WriteHeader(400)
		handler.json.NewEncoder(response).Encode(HttpProblem{
			Message: result.Err.Error(),
		})
		return
	}
	err = handler.json.NewEncoder(response).Encode(NewMessageView(result.Message))
	if err != nil {
		response.WriteHeader(500)
		handler.log.Error(err)
		return
	}
}

func (handler *Handler) GetTopics(response http.ResponseWriter, request *http.Request, params httprouter.Params) {
	names, err := ListAll()
	if err != nil {
		response.WriteHeader(500)
		handler.log.Error(err)
		return
	}
	view := &TopicsView{
		Topics: names,
	}
	err = handler.json.NewEncoder(response).Encode(view)
	if err != nil {
		response.WriteHeader(500)
		handler.log.Error(err)
		return
	}
}
