package handlers

import (
	"bytes"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"kibouse/clickhouse"
	"kibouse/data/wrappers"
	"kibouse/data/models"
	"kibouse/db"
	"kibouse/adapter/requests"
	"kibouse/adapter/responses"
	"kibouse/adapter/requests/aggregations"
)

// MultiSearchHandler returns handler for processing elastic _msearch requests, used for logs data searching and aggregating
func MultiSearchHandler(context HandlerContext) http.HandlerFunc {
	handler := func(w http.ResponseWriter, r *http.Request) {
		body, err := readBody(w, r, context.RuntimeLog)
		if err != nil {
			return
		}

		var response string
		builder := responses.NewFullResponseBuilder(true)

		for len(body) > 0 {

			index, ok := context.URL.FetchParam(r, "index")
			indexJSONEnding := bytes.IndexByte(body, '}')
			nextRequestMarker := []byte(`{}`)
			// index not set in URL, get it from received JSON
			if !ok {
				if indexJSONEnding == -1 {
					writeResponseError(w, errors.New("request body has incorrect format"), http.StatusBadRequest, context.RuntimeLog)
					return
				}

				indexJSON, err := requests.ParseElasticJSON(body[:indexJSONEnding+1], nil)
				if err != nil {
					writeResponseError(w, errors.Wrap(err,"cannot get json with Elastic index from request body"), http.StatusBadRequest, context.RuntimeLog)
					return
				}

				if index = indexJSON.Index; index == "" {
					writeResponseError(w, errors.New("cannot fetch Elastic index from json"), http.StatusBadRequest, context.RuntimeLog)
					return
				}
				nextRequestMarker = []byte(`{"index":`)
			}

			body = body[indexJSONEnding+1:]

			provider, err := clickhouse.NewProvider(index)
			if err != nil {
				writeResponseError(w, err, http.StatusInternalServerError, context.RuntimeLog)
				return
			}

			requestParamsEnding := bytes.Index(body, nextRequestMarker)
			if requestParamsEnding == -1 {
				requestParamsEnding = len(body)
			}

			response, err = executeRequest(body[:requestParamsEnding], provider, builder, context.RuntimeLog)

			if err != nil {
				writeResponseError(w, err, http.StatusInternalServerError, context.RuntimeLog)
				return
			}

			body = body[requestParamsEnding:]
		}

		writeResponseSuccess(w, &response)
	}

	if context.HttpLog != nil {
		return logHttpTransactions(context.HttpLog, handler)
	}
	return handler
}

// SearchHandler returns handler for elastic _search requests,
// required for getting .kibana index mapping and list of existed data tables for 'management' page.
// Also used for some static requests
func SearchHandler(context HandlerContext) http.HandlerFunc {
	handler := func(w http.ResponseWriter, r *http.Request) {
		body, err := readBody(w, r, context.RuntimeLog)
		if err != nil {
			return
		}

		index, ok := context.URL.FetchParam(r, "index")
		if !ok {
			writeResponseError(w, errors.New("cannot fetch index name from url"), http.StatusBadRequest, context.RuntimeLog)
			return
		}

		var response string

		if index == "_all" {
			response = responses.CreateDataNotFoundResponse()
		} else {
			provider, err := clickhouse.NewProvider(index)
			if err != nil {
				writeResponseError(w, err, http.StatusInternalServerError, context.RuntimeLog)
				return
			}
			response, err = executeRequest(
				body,
				provider,
				createResponseBuilder(r, false),
				context.RuntimeLog,
			)
			if err != nil {
				writeResponseError(w, err, http.StatusInternalServerError, context.RuntimeLog)
				return
			}
		}

		writeResponseSuccess(w, &response)
	}
	if context.HttpLog != nil {
		return logHttpTransactions(context.HttpLog, handler)
	}
	return handler
}

func createResponseBuilder(r *http.Request, multipleReq bool) responses.Builder {
	if r == nil {
		return nil
	}
	filter := r.URL.Query()["filter_path"]
	if filter != nil && filter[0] == "aggregations.types.buckets" {
		return responses.NewAggOnlyResponseBuilder()
	}
	return responses.NewFullResponseBuilder(multipleReq)
}

func executeRequest(request []byte, conn db.DataProvider, response responses.Builder, log *logrus.Logger) (string, error) {
	esReq, err := requests.ParseElasticJSON(request, conn.DataScheme())
	if err != nil {
		return "", err
	}

	setResponseParams(&esReq, conn.DataTable(), response)

	aggRes, err := aggregateData(conn, esReq)
	if err != nil {
		return "", err
	}
	response.AddAggregationResult(aggRes)

	if aggRes != nil {
		if newLowerBound, ok := reduceLogsSelectionTimeRange(aggRes.Buckets.Buckets, uint64(esReq.Size)); ok {
			esReq.UpdateLogsLowerTimeRange(newLowerBound)
		}
	}

	hits, err := queryData(conn, esReq)
	if err != nil {
		return "", err
	}
	if hits != nil {
		response.AddHits(hits)
	}

	return response.CreateElasticJSON()
}

// kibana shows only few latest logs entries, so we can skip loading most of it except the MaxLogEntries latest.
func reduceLogsSelectionTimeRange(aggrBuckets []aggregations.Bucket, size uint64) (time.Time, bool) {
	var docCount uint64

	for i := len(aggrBuckets) - 1; i >= 0; i-- {
		docCount += aggrBuckets[i].DocCount()
		if docCount > size {
			return aggrBuckets[i].Key().(time.Time), true
		}
	}
	return time.Now(), false
}

func queryData(conn db.DataProvider, req requests.ElasticRequest) (wrappers.ChDataWrapper, error) {
	clickhouseRequest := clickhouse.NewRequestTpl(conn.DataTable())

	if req.Query != nil {
		clickhouseRequest.Where(req.Query.String())
	}

	// getting kibana settings
	if conn.DataTable() == models.SettingsTableName {
		clickhouseRequest.Final(true)
	} else if req.Size == 0 { // getting logs data
		return nil, nil
	} else {
		clickhouseRequest.Limit(req.Size)
	}

	clickhouseRequest.OrderBy(req.Sorting.String())
	query := clickhouseRequest.Build()

	print("\n\n Data selection request : ", query) // TODO

	return conn.FetchData(clickhouseRequest)
}

func aggregateData(conn db.DataProvider, req requests.ElasticRequest) (*aggregations.BucketAggregationData, error) {
	if req.Aggregations != nil {
		return req.Aggregations.Aggregate(conn)
	}
	return nil, nil
}

func setResponseParams(req *requests.ElasticRequest, table string, response responses.Builder) {
	response.AddIndex(table)
	if len(req.DocValueFields) > 0 {
		response.AddDocValueFields(req.DocValueFields)
	}
	if len(req.SortingFields) > 0 {
		response.AddSorting(req.SortingFields)
	}
}
