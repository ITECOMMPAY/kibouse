package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/pkg/errors"

	"kibouse/clickhouse"
	"kibouse/data/models"
	"kibouse/adapter/responses"
	"kibouse/adapter/requests/queries"
)

// ElasticMappingBuildHandler returns elastic field capabilities json, required for adding
// new indices to kibana using 'management' page
func ElasticMappingBuildHandler(context HandlerContext) http.HandlerFunc {
	handler := func(w http.ResponseWriter, r *http.Request) {
		index, ok := context.URL.FetchParam(r, "index")
		if !ok {
			writeResponseError(w, errors.New("cannot fetch element id from url"), http.StatusBadRequest, context.RuntimeLog)
			return
		}

		provider, err := clickhouse.NewProvider(index)
		if err != nil {
			response := responses.CreateIndexNotFoundResponse(index)
			writeResponseJSON(w, &response, http.StatusNotFound)
			return
		}

		response, err := responses.CreateFieldCapsJSON(provider.DataScheme().DataFields)
		if err != nil {
			writeResponseError(w, err, http.StatusInternalServerError, context.RuntimeLog)
			return
		}

		writeBytesResponseSuccess(w, response)
	}
	if context.HttpLog != nil {
		return logHttpTransactions(context.HttpLog, handler)
	}
	return handler
}

// MultiGetRequestsHandler is the handler for elastic _mget requests
// used for multiple data fetching from kibana settings table
func MultiGetRequestsHandler(context HandlerContext) http.HandlerFunc {
	handler := func(w http.ResponseWriter, r *http.Request) {
		body, err := readBody(w, r, context.RuntimeLog)
		if err != nil {
			return
		}
		requiredIds, err := parseRequiredIdsList(body)
		if err != nil || len(requiredIds) == 0 {
			writeResponseError(w, errors.New("cannot get list of required ids from uri"), http.StatusBadRequest, context.RuntimeLog)
			return
		}
		index := requiredIds[0].Index
		for i := range requiredIds {
			if index != requiredIds[i].Index {
				writeResponseError(w, errors.New("getting from multiple indices not supported"), http.StatusBadRequest, context.RuntimeLog)
				return
			}
		}

		provider, err := clickhouse.NewProvider(index)
		if err != nil {
			writeResponseError(w, err, http.StatusInternalServerError, context.RuntimeLog)
			return
		}

		req := clickhouse.NewRequestTpl(provider.DataTable()).
			What("*").
			Final(provider.DataTable() == models.SettingsTableName).
			Limit(len(requiredIds))

		for i := range requiredIds {
			req.WhereOr(queries.NewStringMatch("_id", requiredIds[i].ID).String())
		}

		response := responses.NewDocItemsResponseBuilder()

		response.AddIndex(provider.DataTable())

		rows, err := provider.FetchData(req)
		if err != nil {
			writeResponseError(w, err, http.StatusInternalServerError, context.RuntimeLog)
			return
		}
		response.AddHits(rows)

		result, err := response.CreateElasticJSON()
		if err != nil {
			writeResponseError(w, err, http.StatusInternalServerError, context.RuntimeLog)
			return
		}
		writeResponseSuccess(w, &result)
	}
	if context.HttpLog != nil {
		return logHttpTransactions(context.HttpLog, handler)
	}
	return handler
}

// StaticRequestsHandler returns predefined static elastic response for provided uri
func StaticRequestsHandler(context HandlerContext) http.HandlerFunc {
	handler := func(w http.ResponseWriter, r *http.Request) {
		response, ok := context.Cfg.StaticResponse(r.RequestURI)
		if !ok {
			context.RuntimeLog.Debugf("unsupported request: %s", r.RequestURI)
		}
		writeResponseSuccess(w, &response)
	}

	return handler
}

type itemIdentifier struct {
	Index string `json:"_index"`
	ID    string `json:"_id"`
	Type  string `json:"_type"`
}

func parseRequiredIdsList(bytes []byte) ([]itemIdentifier, error) {
	ids := struct {
		Docs []itemIdentifier `json:"docs"`
	}{}
	err := json.Unmarshal(bytes, &ids)
	if err != nil {
		return nil, err
	}
	return ids.Docs, nil
}
