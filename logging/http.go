package logging

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"strings"
	"sync"
)

const delim = "\n--------------------------------------------------------------\n"

type HttpLog struct {
	sync.Mutex
	reqId  uint
	logger *log.Logger
}

func NewHttpLogger(logger *log.Logger) *HttpLog {
	return &HttpLog{logger: logger}
}

func (hl *HttpLog) DumpRequest(request *http.Request) uint {
	if hl.logger == nil {
		return 0
	}
	hl.Lock()
	hl.reqId++
	message := strings.Builder{}
	message.WriteString(delim)
	message.WriteString(fmt.Sprintf("\nRequest No %d:", hl.reqId))
	message.WriteString("\npath : " + request.URL.Path)
	body, err := httputil.DumpRequest(request, true)
	if err != nil {
		message.WriteString("\n error while getting request body in debug adapter: " + err.Error())
	} else {
		message.WriteString("\nRequest Body : " + string(body))
	}
	hl.logger.Println(message.String())
	hl.Unlock()
	return hl.reqId
}

func (hl *HttpLog) DumpResponse(response *http.Response, id uint) {
	if hl.logger == nil {
		return
	}
	hl.Lock()
	message := strings.Builder{}
	message.WriteString(delim)
	message.WriteString(fmt.Sprintf("\nResponse No %d:", id))
	body, err := httputil.DumpResponse(response, true)
	if err != nil {
		message.WriteString("\n error while getting response body in debug adapter: " + err.Error())
	} else {
		message.WriteString("\nResponse Body : " + string(body))
	}
	hl.logger.Println(message.String())
	hl.Unlock()
}
