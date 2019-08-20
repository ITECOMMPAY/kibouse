package handlers

import (
	"net/http"

	"kibouse/logging"
)

// LoggingElasticReverseProxy creates reverse proxy that logs all requests to elasticsearch.
func LoggingElasticReverseProxy(context HandlerContext) http.HandlerFunc {
	proxy := logging.NewReverseProxy(context.Cfg.GetElasticSource(), context.HttpLog)
	return proxy.Handle
}


