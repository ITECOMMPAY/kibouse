package requests

import (
	"encoding/json"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"kibouse/data/models"
	"kibouse/adapter/requests/queries"
	"kibouse/adapter/requests/aggregations"
)

// ElasticRequest parses JSON requests to elasticsearch.
type ElasticRequest struct {
	config       map[string]interface{}
	tableInfo    *models.ModelInfo
	ranges       map[string]*queries.RangeClause
	matchQueries []*queries.MatchQueryClause
	Index          string
	Size           int
	Query          queries.Clause
	Sorting        queries.SortSection
	SortingFields  []string
	DocValueFields []string
	Aggregations   aggregations.Aggregation
}

// UpdateLogsLowerTimeRange sets new logs time range lower bound.
func (req *ElasticRequest) UpdateLogsLowerTimeRange(low time.Time) {
	if timeRange := req.getLogsTimeRange(); timeRange != nil {
		timeRange.AddLower(float64(low.UnixNano()), true)
		req.addTimeRangesToQuery()
	}
}

// ParseElasticJSON parses request to elasticsearch.
func ParseElasticJSON(jsonCfg []byte, tableInfo *models.ModelInfo) (elasticCfg ElasticRequest, err error) {
	if err = json.Unmarshal(jsonCfg, &elasticCfg.config); err != nil {
		return
	}

	if tableInfo != nil {
		elasticCfg.tableInfo = tableInfo
	} else {
		elasticCfg.tableInfo = &models.ModelInfo{
			DataFields: make(map[string]*models.FieldProps),
		}
	}

	elasticCfg.ranges = make(map[string]*queries.RangeClause)
	elasticCfg.SortingFields = make([]string, 0)
	elasticCfg.DocValueFields = make([]string, 0)
	elasticCfg.matchQueries = make([]*queries.MatchQueryClause, 0)

	// json with elasticsearch index doesn't contain any important data
	if elasticCfg.fetchIndex() {
		return
	}

	elasticCfg.fetchSize()
	elasticCfg.fetchSortingInfo()
	elasticCfg.fetchDocValueFields()
	elasticCfg.fetchQuery()
	elasticCfg.fetchAggregationSettings()

	elasticCfg.addTimeRangesToQuery()
	return
}

func (req *ElasticRequest) getLogsTimeRange() *queries.RangeClause {
	if timestampField, ok := req.tableInfo.GetTimestampField(); ok {
		return req.ranges[timestampField.KibanaName]
	}
	return nil
}

// when we use full text searching with MatchQueryClause we should set time ranges
// (required for fast data fetching from inverted index)
func (req *ElasticRequest) addTimeRangesToQuery() {
	if timeRange := req.getLogsTimeRange(); timeRange != nil {
		for i := range req.matchQueries {
			req.matchQueries[i].SetTimeRange(*timeRange)
		}
	}
}

func (req *ElasticRequest) fetchSize() {
	if param, found := fetchJsonParamFromMap("size", req.config); found {
		// go interprets all numbers in json as float64
		if size, ok := param.(float64); ok {
			req.Size = int(size)
		}
	}
}

func (req *ElasticRequest) fetchIndex() bool {
	if param, found := fetchJsonParamFromMap("index", req.config); found {
		if index, ok := param.(string); ok {
			req.Index = index
			return true
		}
		if indices, ok := param.([]interface{}); ok {
			if index, ok := indices[0].(string); ok {
				req.Index = index
				return true
			}
		}
	}
	return false
}

func (req *ElasticRequest) fieldExists(name string) bool {
	_, ok := req.tableInfo.DataFields[name]
	return ok
}

// fetchSortingInfo parses section with info about requested data sorting
// "sort":[{<field_name>:{"order":<asc\desc>}}, ...]
func (req *ElasticRequest) fetchSortingInfo() {
	sortCfg, ok := fetchJsonParamFromMap("sort", req.config)
	if !ok {
		return
	}
	sortCfgArr := sortCfg.([]interface{})

	req.Sorting = queries.SortSection{}
	for i := range sortCfgArr {
		fieldSorting := sortCfgArr[i].(map[string]interface{})
		for fieldName := range fieldSorting {
			correctedName := correctFieldName(fieldName)
			if !req.fieldExists(correctedName) {
				continue
			}
			fieldSortingCfg := fieldSorting[fieldName].(map[string]interface{})
			req.SortingFields = append(req.SortingFields, correctedName)
			req.Sorting.AppendChild(&queries.SortClause{Field: correctedName, Sorting: fieldSortingCfg["order"].(string)})
		}
	}
}

// fetchDocValueFields fetches list of document value fields from elasticsearch request json.
func (req *ElasticRequest) fetchDocValueFields() {
	docFields, ok := fetchJsonParamFromMap("docvalue_fields", req.config)
	if !ok {
		return
	}
	docFieldsArr := docFields.([]interface{})
	req.DocValueFields = make([]string, len(docFieldsArr))
	for i := range docFieldsArr {
		req.DocValueFields[i] = correctFieldName(docFieldsArr[i].(string))
	}
}

// fetchQuery parses elastic request section "query".
func (req *ElasticRequest) fetchQuery() {
	if queryCfg, ok := fetchJsonParamFromMap("query", req.config); !ok {
		return
	} else {
		var clauses queries.Clause
		if boolCfg, ok := fetchJsonParamFromInterface("bool", queryCfg); ok {
			clauses = req.parseBool(boolCfg)
		} else {
			clauses = req.parseSimpleQueryClause(queryCfg)
		}
		req.Query = clauses
	}
}


func (req *ElasticRequest) parseBool(config interface{}) queries.Clause {
	if boolCfg, ok := config.(map[string]interface{}); ok {
		boolClause := queries.BoolSection{
			Must:    req.parseBoolSection(boolCfg["must"], &queries.MustSection{}),
			MustNot: req.parseBoolSection(boolCfg["must_not"], &queries.MustNotSection{}),
			Should:  req.parseBoolSection(boolCfg["should"], &queries.ShouldSection{}),
			Filter:  req.parseBoolSection(boolCfg["filter"], &queries.FilterSection{}),
		}
		return &boolClause
	} else {
		log.Warnf("query 'bool' has incorrect format")
		return &queries.UnknownClause{}
	}
}

func (req *ElasticRequest) parseBoolSection(config interface{}, section queries.Section) queries.Section {
	if clauses, ok := config.([]interface{}); !ok {
		return &queries.EmptySection{}
	} else {
		for _, clause := range clauses {
			section.AppendChild(req.parseSimpleQueryClause(clause))
		}
	}
	return section
}

func (req *ElasticRequest) parseSimpleQueryClause(config interface{}) queries.Clause {
	clause := config.(map[string]interface{})
	for key, value := range clause {
		switch key {
		case "exists":
			return req.parseExists(value)
		case "match_phrase":
			return req.parseMatchPhrase(value)
		case "range":
			return req.parseRange(value)
		case "query_string":
			return req.parseQueryString(value)
		case "bool":
			return req.parseBool(value)
		case "terms":
			return req.parseTerms(value)
		case "term":
			return req.parseTerms(value)
		case "match_all":
			// no special conditions required
			return nil
		}
	}

	return &queries.UnknownClause{}
}

func (req *ElasticRequest) parseExists(config interface{}) queries.Clause {
	if field, ok := fetchJsonParamFromInterface("field", config); ok {
		if fieldStr, ok := field.(string); ok {
			return &queries.ExistsClause{Field: correctFieldName(fieldStr)}
		}
	}
	log.Warnf("couldn't parse query 'exists' clause")
	return &queries.UnknownClause{}
}

func (req *ElasticRequest) parseMatchPhrase(config interface{}) queries.Clause {
	if match, ok := config.(map[string]interface{}); ok {
		for fieldName := range match {
			// match_phrase section could be like this: "match_phrase": { "name": "Bob" }
			// or like this: "match_phrase": { "name": { "query": "Bob" } }
			name := correctFieldName(fieldName)
			if fieldInfo, ok := req.tableInfo.DataFields[name]; ok {
				if matchValue, ok := match[fieldName].(map[string]interface{}); ok {
					return queries.NewMatchClause(fieldInfo.CHField, matchValue["query"])
				} else {
					return queries.NewMatchClause(fieldInfo.CHField, match[fieldName])
				}
			}
		}
	}
	log.Warnf("couldn't parse query 'match_phrase' clause")
	return &queries.UnknownClause{}
}

func (req *ElasticRequest) parseRange(config interface{}) queries.Clause {
	if rangeMap, ok := config.(map[string]interface{}); ok {
		for fieldName := range rangeMap {
			name := correctFieldName(fieldName)
			field, ok := req.tableInfo.DataFields[name]
			if !ok {
				break
			}
			rangeClause := queries.NewRange(name, field.IsArray())
			if rangeParams, ok := rangeMap[fieldName].(map[string]interface{}); ok {
				req.ranges[name] = fetchRangeParams(rangeParams, rangeClause)
				return req.ranges[name]
			}
		}
	}
	log.Warnf("couldn't parse query 'range' clause")
	return &queries.UnknownClause{}
}

func convertByFormat(val float64, format string) float64 {
	// converting milliseconds (elasticsearch timestamp format) to nanoseconds (logs timestamp format) for range of time
	if format == "epoch_millis" {
		return val * 1000000
	}
	return val
}

func fetchRangeParams(config map[string]interface{}, rc *queries.RangeClause) *queries.RangeClause {
	if format, ok := config["format"]; ok {
		rc.AddFormat(format.(string))
	}
	for name, val := range config {
		switch name {
		case "lt":
			rc.AddUpper(convertByFormat(val.(float64), rc.GetFormat()), true)
		case "lte":
			rc.AddUpper(convertByFormat(val.(float64), rc.GetFormat()), false)
		case "gt":
			rc.AddLower(convertByFormat(val.(float64), rc.GetFormat()), true)
		case "gte":
			rc.AddLower(convertByFormat(val.(float64), rc.GetFormat()), false)
		}
	}
	return rc
}

func (req *ElasticRequest) parseQueryString(config interface{}) queries.Clause {
	if queryStringCfg, ok := config.(map[string]interface{}); ok {
		analyzeWildCard := false
		if wildCard, ok := queryStringCfg["analyze_wildcard"]; ok {
			analyzeWildCard = wildCard.(bool)
		}
		if q, ok := queryStringCfg["query"]; ok {
			if matchQuery := queries.NewMatchQueryClause(q.(string), analyzeWildCard, req.tableInfo); matchQuery != nil {
				// store pointer to MatchQueryClause, because we still need time range condition for inverted index request.
				// it should be added later from query range clause.
				req.matchQueries = append(req.matchQueries, matchQuery)
				return matchQuery
			}
		} else {
			log.Warnf("couldn't find condition for 'query_string' clause")
			return &queries.UnknownClause{}
		}
	}
	log.Warnf("couldn't parse query 'query_string' clause")
	return &queries.UnknownClause{}
}

func (req *ElasticRequest) parseTerms(config interface{}) queries.Clause {
	if termsCfg, ok := config.(map[string]interface{}); ok {
		terms := queries.NewTermsClause()
		for fieldName := range termsCfg {
			if !req.fieldExists(fieldName) {
				continue
			}
			if fieldVals, ok := termsCfg[fieldName].([]interface{}); ok {
				for i := range fieldVals {
					terms.AddTerm(fieldName, fieldVals[i].(string))
				}
			} else if fieldVal, ok := termsCfg[fieldName].(string); ok {
				terms.AddTerm(fieldName, fieldVal)
			} else {
				log.Warnf("couldn't parse field values list in terms clause")
			}
		}
		return terms
	}
	log.Warnf("couldn't parse query 'terms' clause")
	return &queries.UnknownClause{}
}

func (req *ElasticRequest) fetchAggregationSettings() {
	req.Aggregations = req.parseAggregation(req.config)
}

func (req *ElasticRequest) parseAggregation(config map[string]interface{}) aggregations.Aggregation {
	aggs, ok := fetchJsonParamFromMap("aggs", config)
	if !ok {
		return nil
	}
	if aggsMap, ok := aggs.(map[string]interface{}); ok {
		if len(aggsMap) > 1 {
			log.Warnf("more than one aggregation found on single nesting level")
		}
		for aggName := range aggsMap {
			aggregation := aggsMap[aggName]
			if aggSettings, ok := aggregation.(map[string]interface{}); ok {
				aggregation := req.parseAggregationSettings(aggSettings)
				if aggregation != nil {
					aggregation.SetAggName(aggName)
				}
				return aggregation
			}
		}
	}
	return nil
}

func (req *ElasticRequest) parseAggregationSettings(aggSettings map[string]interface{}) aggregations.Aggregation {
	var agg aggregations.Aggregation = nil
	for aggType := range aggSettings {
		switch aggType {
		case "date_histogram":
			agg = req.parseDateHistogramSettings(aggSettings[aggType])
		case "filters":
			agg = req.parseFiltersSettings(aggSettings[aggType])
		}
	}

	if agg != nil {
		agg.SetSubAgg(req.parseAggregation(aggSettings))
		agg.AddCommonFilter(req.Query)
	}

	return agg
}

func (req *ElasticRequest) parseDateHistogramSettings(settings interface{}) aggregations.Aggregation {
	if histogramCfg, ok := settings.(map[string]interface{}); ok {
		field, ok := histogramCfg["field"].(string)
		if !ok {
			log.Warnf("couldn't find timestamp field for histogram aggregation")
			return nil
		}
		interval, ok := histogramCfg["interval"].(string)
		if !ok {
			log.Warnf("couldn't find interval field for histogram aggregation")
			return nil
		}
		correctedName := correctFieldName(field)

		if fieldRange, ok := req.ranges[correctedName]; ok {
			agg, err := aggregations.CreateDateHistogramAgg(interval, correctedName, fieldRange, req.Size > 0)
			if err != nil {
				log.Warnf(err.Error())
				return nil
			}
			return agg
		} else {
			log.Warnf("couldn't find time range settings for histogram")
			return nil
		}
	}

	return nil
}

func (req *ElasticRequest) parseFiltersSettings(settings interface{}) aggregations.Aggregation {
	// filters section has the following format
	//"filters": {
	//	"filters": {
	//		"1": {
	//
	//		},
	//		...
	//	}
	//}
	if filters, ok := settings.(map[string]interface{}); ok {
		if filtersListCfg, ok := fetchJsonParamFromMap("filters", filters); ok {
			return req.parseFiltersList(filtersListCfg)
		}
	}
	return nil
}

func (req *ElasticRequest) parseFiltersList(settings interface{}) *aggregations.Filters {
	filters := make([]aggregations.FilterSettings, 0)
	if filtersListCfg, ok := settings.(map[string]interface{}); ok {
		for name, condition := range filtersListCfg {
			filters = append(filters, aggregations.FilterSettings{Name: name, Condition: req.parseSimpleQueryClause(condition)})
		}
		return aggregations.CreateFiltersAgg(filters)
	}
	log.Warnf("couldn't parse filters aggregation settings")
	return nil
}

func fetchJsonParamFromMap(name string, config map[string]interface{}) (interface{}, bool) {
	if param, ok := config[name]; ok {
		return param, ok
	} else {
		return nil, false
	}
}

func fetchJsonParamFromInterface(name string, config interface{}) (interface{}, bool) {
	if paramsMap, ok := config.(map[string]interface{}); ok {
		return fetchJsonParamFromMap(name, paramsMap)
	} else {
		return nil, false
	}
}

// clickhouse doesn't support elasticsearch keyword type (name.keyword) used for search by keywords,
// so we remove it from field name
// also clickhouse doesn't support char '@'
func correctFieldName(name string) string {
	name = strings.Replace(name, ".keyword", "", 1)
	return strings.Replace(name, "@", "", 1)
}
