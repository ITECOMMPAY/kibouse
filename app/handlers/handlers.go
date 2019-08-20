package handlers

import (
	"fmt"
	"bytes"
	"io/ioutil"
	"net/http"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"kibouse/config"
	"kibouse/logging"

)

type urlParser interface {
	FetchParam(*http.Request, string) (string, bool)
}

// HandlerConstructor is a function for binding server app context
// (configuration settings, external data storage connections, etc.) with request handler
type HandlerConstructor func(HandlerContext) http.HandlerFunc

// HandlerContext provide server application context for request handlers
type HandlerContext struct {
	URL        urlParser
	RuntimeLog *logrus.Logger
	HttpLog    *logging.HttpLog
	Cfg        *config.AppConfig
}

func readBody(w http.ResponseWriter, r *http.Request, log *logrus.Logger) ([]byte, error) {
	body, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		writeResponseError(w, errors.New("error in getting request body"), http.StatusBadRequest, log)
		return nil, err
	}
	return body, nil
}

func writeResponseError(w http.ResponseWriter, err error, code int, log *logrus.Logger) {
	log.Error(err.Error())
	log.Error(fmt.Printf("%+v\n", err))
	http.Error(w, err.Error(), code)
}

func writeResponseSuccess(w http.ResponseWriter, body *string) {
	writeResponseJSON(w, body, http.StatusOK)
}

func writeResponseJSON(w http.ResponseWriter, body *string, code int) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(code)

	fmt.Fprint(w, *body)
}

func writeBytesResponseSuccess(w http.ResponseWriter, body []byte) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)

	w.Write(body)
}

type loggingResponseWriter struct {
	http.ResponseWriter
	body   bytes.Buffer
	status int
	length int
}

func (w *loggingResponseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *loggingResponseWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = 200
	}
	n, err := w.ResponseWriter.Write(b)
	w.body.Write(b)
	w.length += n
	return n, err
}

func (w *loggingResponseWriter) GetResponse() *http.Response {
	return &http.Response{
		Status:        " ",
		StatusCode:    w.status,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Body:          ioutil.NopCloser(&w.body),
		ContentLength: int64(w.length),
		Request:       nil,
		Header:        w.Header(),
	}
}

func logHttpTransactions(logger *logging.HttpLog, handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		lw := loggingResponseWriter{ResponseWriter: w}
		id := logger.DumpRequest(r)
		handler(&lw, r)
		logger.DumpResponse(lw.GetResponse(), id)
	}
}
