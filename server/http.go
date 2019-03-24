package main

import (
	"net/http"

	jsoniter "github.com/json-iterator/go"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
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

func NewMessageView(value []byte) *MessageView {
	if value == nil {
		return &MessageView{}
	}
	return &MessageView{
		Message: string(value),
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
	writer, err := handler.bus.GetWriter(params.ByName("name"))
	if err != nil {
		handler.log.Error(err)
		response.WriteHeader(500)
		return
	}
	_, err = writer.Append([]byte(writeCommand.Message))
	if err != nil {
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

	reader, err := handler.bus.GetReader(name, group)
	if err != nil {
		response.WriteHeader(500)
		return
	}
	receiver := reader.Join(clientID)
	message := receiver.Next()
	message.AckOK()

	err = handler.json.NewEncoder(response).Encode(NewMessageView(message.Value))
	if err != nil {
		response.WriteHeader(500)
		handler.log.Error(err)
		return
	}
}

// func (handler *Handler) GetTopics(response http.ResponseWriter, request *http.Request, params httprouter.Params) {
// 	names, err := ListAll()
// 	if err != nil {
// 		response.WriteHeader(500)
// 		handler.log.Error(err)
// 		return
// 	}
// 	view := &TopicsView{
// 		Topics: names,
// 	}
// 	err = handler.json.NewEncoder(response).Encode(view)
// 	if err != nil {
// 		response.WriteHeader(500)
// 		handler.log.Error(err)
// 		return
// 	}
// }
