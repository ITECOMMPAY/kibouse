package logging

import (
	"net/url"
	"net/http/httputil"
	"net/http"
)

// ReverseProxy is a reverse proxy that logs all incoming requests.
type ReverseProxy struct {
	target *url.URL
	proxy  *httputil.ReverseProxy
}

// Handle serves incoming http requests.
func (p *ReverseProxy) Handle(w http.ResponseWriter, r *http.Request) {
	p.proxy.ServeHTTP(w, r)
}

// NewReverseProxy creates new logging reverse proxy.
func NewReverseProxy(target string, logger *HttpLog) *ReverseProxy {
	url, _ := url.Parse(target)
	proxy := httputil.NewSingleHostReverseProxy(url)
	proxy.Transport = &transport{
		log: logger,
	}
	return &ReverseProxy{target: url, proxy: proxy}
}

type transport struct {
	log *HttpLog
}

func (t *transport) RoundTrip(request *http.Request) (*http.Response, error) {
	id := t.log.DumpRequest(request)
	response, err := http.DefaultTransport.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	t.log.DumpResponse(response, id)
	return response, nil
}