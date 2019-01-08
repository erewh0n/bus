package main

import (
	"net/http"

	"go.uber.org/zap"

	jsoniter "github.com/json-iterator/go"
	"github.com/julienschmidt/httprouter"
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
	bus  *Bus
	log  *zap.SugaredLogger
	json jsoniter.API
}

func NewHandler(bus *Bus, log *zap.SugaredLogger) *Handler {
	return &Handler{
		bus:  bus,
		log:  log,
		json: jsoniter.ConfigCompatibleWithStandardLibrary,
	}
}

func NewMessageView(message *Message) *MessageView {
	if message == nil {
		return &MessageView{}
	}
	return &MessageView{
		Message: message.Body,
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

	result := handler.bus.Send(params.ByName("name"), writeCommand.Message)
	if result.Err != nil {
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

	messenger, err := handler.bus.Receive(name, group, clientID)
	if err != nil {
		response.WriteHeader(500)
		return
	}
	result := <-messenger
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
