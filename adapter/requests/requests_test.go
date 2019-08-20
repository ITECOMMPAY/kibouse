package requests

import (
	"fmt"
	"testing"
	"time"
	"reflect"

	"github.com/stretchr/testify/assert"

	"kibouse/data/models"
	"kibouse/adapter/requests/queries"
)

type gate struct {
	UUID                 uint64    `db:"uuid" json:"uuid" type:"UInt64" uuid:"true" mv_transform:"cityHash64(uuid)" base_type:"String"`
	Day                  time.Time `db:"day" json:"day" type:"Date" partitioning:"true" mv_transform:"today()"`
	TS                   uint64    `db:"ts" json:"ts" type:"UInt64" timestamp:"true" ch_index_pos:"1"`
	TsOriginal           string    `db:"ts_original" json:"ts_original" type:"String"`
	Type                 string    `db:"type" json:"type" type:"String"`
	Pid                  uint64    `db:"pid" json:"pid" type:"UInt64"`
	RemoteIP             string    `db:"remote_ip" json:"remote_ip" type:"String" default:""`
	Hostname             string    `db:"hostname" json:"hostname" type:"String"`
	Status               string    `db:"status" json:"status" type:"String"`
	File                 string    `db:"file" json:"file" type:"String" inv_index:"true"`
	Line                 uint16    `db:"line" json:"line" type:"UInt16"`
	Message              string    `db:"message" json:"message" type:"String" inv_index:"true"`
	LoggerID             string    `db:"logger_id" json:"logger_id" type:"String" default:""`
	IsBusinessLog        uint64    `db:"is_business_log" json:"is_business_log" type:"UInt64" default:"0"`
	PhpExecutionLoggerID string    `db:"php_execution_logger_id" json:"php_execution_logger_id" type:"String" default:"" inv_index:"true"`
	JobLoggerID          string    `db:"job_logger_id" json:"job_logger_id" type:"String" default:"" inv_index:"true"`
	SpanID               string    `db:"span_id" json:"span_id" type:"String" default:""`
	ParesEncoded         string    `db:"pares_encoded" json:"pares_encoded" type:"String" default:""`
	Pares                string    `db:"pares" json:"pares" type:"String" default:""`
	ParesXML             string    `db:"pares_xml" json:"pares_xml" type:"String" default:""`
	Error                string    `db:"error" json:"error" type:"String" default:""`
	ErrorType            string    `db:"error_type" json:"error_type" type:"String" default:""`
	Source               string    `db:"source" json:"source" type:"String" inv_index:"true"`
	Offset               string    `db:"offset" json:"offset" type:"UInt64"`
	Table                string    `db:"_table" type:"String" json:"table" skip:"db"`
}

const typicalIndexJSON = `
{
  "index": [
    "logs_2p_gate"
  ],
  "ignore_unavailable": true,
  "preference": 1563192790772
}`

const typicalRequestJSON = `{
  "version": true,
  "size": 500,
  "sort": [
    {
      "ts": {
        "order": "desc",
        "unmapped_type": "boolean"
      }
    }
  ],
  "query": {
    "bool": {
      "must": [
        {
          "match_all": {}
        },
        {
          "range": {
            "ts": {
              "gte": 1563191891606,
              "lte": 1563192791606,
              "format": "epoch_millis"
            }
          }
        }
      ],
      "must_not": []
    }
  },
  "_source": {
    "excludes": []
  },
  "aggs": {
    "2": {
      "date_histogram": {
        "field": "ts",
        "interval": "30s",
        "time_zone": "Europe/Minsk",
        "min_doc_count": 1
      }
    }
  },
  "stored_fields": [
    "*"
  ],
  "docvalue_fields": [
    "day",
    "ts"
  ]
}
`

func compareSliceContent(asserts *assert.Assertions, first []string, second []string, descr string) bool {
	firstMap := make(map[string]int)
	for _, str := range first {
		firstMap[str]++
	}

	secondMap := make(map[string]int)
	for _, str := range second {
		secondMap[str]++
	}

	return asserts.Equal(firstMap, secondMap, descr)
}

func caseName(index int, description string, attr string) string {
	return fmt.Sprintf("test case %d: %s \n compare %s", index, description, attr)
}

func emptyCfg() ElasticRequest {
	return ElasticRequest{
		tableInfo: &models.ModelInfo{
			DataFields: make(map[string]*models.FieldProps),
		},
		ranges: make(map[string]*queries.RangeClause),
		SortingFields: make([]string, 0),
		DocValueFields: make([]string, 0),
		matchQueries: make([]*queries.MatchQueryClause, 0),
		config: make(map[string]interface{}),
	}
}

func emptyCfgWithIndex(index string) ElasticRequest {
	cfg := emptyCfg()
	cfg.Index = index
	return cfg
}

func emptyCfgWithSize(size int) ElasticRequest {
	cfg := emptyCfg()
	cfg.Size = size
	return cfg
}

func emptyCfgWithSorting(sorting map[string]queries.Order) ElasticRequest {
	cfg := emptyCfg()
	cfg.Sorting = *queries.NewSortSection(sorting)
	for field := range sorting {
		cfg.SortingFields = append(cfg.SortingFields, field)
	}
	return cfg
}

func emptyCfgWithQuery(query queries.Clause) ElasticRequest {
	cfg := emptyCfg()
	cfg.Query = query
	return cfg
}

func TestParseElasticJSON(t *testing.T) {
	dbFieldsMapping, _ := models.CreateDBFieldsInfoMap(reflect.TypeOf(gate{}))
	gateModel := models.ModelInfo{
		DBName:     "gate",
		DataFields: dbFieldsMapping,
	}

	tests := []struct{
		descr      string
		request   []byte
		tableInfo *models.ModelInfo
		parsedCfg ElasticRequest
		err       bool
	}{
		{
			descr: "empty elastic request",
			err: true,
		},
		{
			descr:    "no model info, empty json",
			request: []byte("{}"),
			parsedCfg: emptyCfg(),
			err: false,
		},
		{
			descr: "fetch index",
			request: []byte(typicalIndexJSON),
			parsedCfg: emptyCfgWithIndex("logs_2p_gate"),
		},
		{
			descr: "incorrect size",
			request: []byte(`{"size": "500"}`),
			parsedCfg: emptyCfg(),
		},
		{
			descr: "fetch size",
			request: []byte(`{"size": 500}`),
			parsedCfg: emptyCfgWithSize(500),
		},
		{
			descr: "fetch sorting without model description",
			request: []byte(`{"sort":[{"ts":{"order":"desc"}},{"@uuid":{"order":"asc"}},{"hostname.keyword":{"order":"asc"}}]}`),
			parsedCfg: emptyCfg(),
		},
		{
			descr: "fetch sorting",
			request: []byte(`{"sort":[{"ts":{"order":"desc"}},{"@uuid":{"order":"asc"}},{"hostname.keyword":{"order":"asc"}}]}`),
			tableInfo: &gateModel,
			parsedCfg: emptyCfgWithSorting(
				map[string]queries.Order{
					"ts": queries.Desc,
					"uuid": queries.Asc,
					"hostname": queries.Asc,
				},
			),
		},
		{
			descr: "fetch kibana match filter condition for unknown data attribute",
			request: []byte(`{"query":{"match_phrase":{"pd":{"query":41671}}}}`),
			tableInfo: &gateModel,
			parsedCfg: emptyCfgWithQuery(&queries.UnknownClause{}),
		},
		{
			descr: "fetch kibana match filter condition with nested query attr",
			request: []byte(`{"query":{"match_phrase":{"pid":{"query":41671}}}}`),
			tableInfo: &gateModel,
			parsedCfg: emptyCfgWithQuery(queries.NewMatchClause(gateModel.DataFields["pid"].CHField, 41671)),
		},
		{
			descr: "fetch kibana match filter condition",
			request: []byte(`{"query":{"match_phrase":{"pid":41671}}}`),
			tableInfo: &gateModel,
			parsedCfg: emptyCfgWithQuery(queries.NewMatchClause(gateModel.DataFields["pid"].CHField, 41671)),
		},
		{
			descr: "fetch kibana match filter condition with nested query attr",
			request: []byte(`{"query":{"match_phrase":{"@pid.keyword":{"query":41671}}}}`),
			tableInfo: &gateModel,
			parsedCfg: emptyCfgWithQuery(queries.NewMatchClause(gateModel.DataFields["pid"].CHField, 41671)),
		},
		{
			descr: "fetch kibana data range condition for unknown data attribute",
			request: []byte(`{"query":{"range":{"tsdf":{"gte":1560124800000,"lte":1560211200000,"format":"epoch_millis"}}}}`),
			tableInfo: &gateModel,
			parsedCfg: emptyCfgWithQuery(&queries.UnknownClause{}),
		},
		{
			descr: "fetch kibana data range condition",
			request: []byte(`{"query":{"range":{"ts":{"gt":1560124800000,"lte":1560211200000}}}}`),
			tableInfo: &gateModel,
			parsedCfg: emptyCfgWithQuery(
				queries.NewRange("ts", false).
					AddLower(1560124800000, true).
					AddUpper(1560211200000, false).
					AddFormat("epoch_millis")),
		},
		{
			descr: "fetch kibana time in ms range condition",
			request: []byte(`{"query":{"range":{"@ts.keyword":{"gte":1560124800000,"lt":1560211200000,"format":"epoch_millis"}}}}`),
			tableInfo: &gateModel,
			parsedCfg: emptyCfgWithQuery(
				queries.NewRange("ts", false).
					AddLower(1560124800000000000, false).
					AddUpper(1560211200000000000, true).
					AddFormat("epoch_millis")),
		},
		{
			descr: "skipping match_all clause",
			request: []byte(`{"query":{"match_all":{}}}`),
			tableInfo: &gateModel,
			parsedCfg: emptyCfg(),
		},
		{
			descr: "fetch attribute existence filter",
			request: []byte(`{"query":{"exists":{"field":"pid"}}}`),
			tableInfo: &gateModel,
			parsedCfg: emptyCfgWithQuery(&queries.ExistsClause{Field: "pid"}),
		},
		{
			descr: "fetch multiple kibana filtering clauses",
			request: []byte(`{"query":{"bool":{"must":[{"match_all":{}},{"match_phrase":{"pid":{"query":41671}}},{"range":{"line":{"gte":100,"lt":500}}},{"bool":{"minimum_should_match":1,"should":[{"match_phrase":{"line":"100"}},{"match_phrase":{"line":"150"}},{"match_phrase":{"line":"200"}},{"match_phrase":{"line":"250"}},{"match_phrase":{"line":"300"}}]}},{"exists":{"field":"pid"}},{"range":{"ts":{"gte":1560124800000,"lte":1560211200000,"format":"epoch_millis"}}}],"must_not":[{"match_phrase":{"is_business_log":{"query":1}}},{"range":{"offset":{"gte":0,"lt":1000000}}}]}}}`),
			tableInfo: &gateModel,
			parsedCfg: emptyCfgWithQuery(&queries.BoolSection{
				Must: queries.NewEmptyMustSection().
					AppendChild(queries.NewMatchClause(gateModel.DataFields["pid"].CHField, 41671)).
					AppendChild(queries.NewRange("line", false).AddUpper(500, true).AddLower(100, false)).
					AppendChild(
						queries.NewRange("ts", false).
							AddLower(1560124800000000000, false).
							AddUpper(1560211200000000000, false).
							AddFormat("epoch_millis")).
					AppendChild(&queries.ExistsClause{Field: "pid"}).
					AppendChild(&queries.BoolSection{
						Should: queries.NewEmptyShouldSectionn().
							AppendChild(queries.NewMatchClause(gateModel.DataFields["line"].CHField, 100)).
							AppendChild(queries.NewMatchClause(gateModel.DataFields["line"].CHField, 150)).
							AppendChild(queries.NewMatchClause(gateModel.DataFields["line"].CHField, 200)).
							AppendChild(queries.NewMatchClause(gateModel.DataFields["line"].CHField, 250)).
							AppendChild(queries.NewMatchClause(gateModel.DataFields["line"].CHField, 300)),
					}),
				MustNot: queries.NewEmptyMustNotSection().
					AppendChild(queries.NewRange("offset", false).AddUpper(1000000, true).AddLower(0, false)).
					AppendChild(queries.NewMatchClause(gateModel.DataFields["is_business_log"].CHField, 1)),
			}),
		},
		{
			descr: "fetch conditions from search query for unknown data attributes",
			request: []byte(`{"query":{"query_string":{"query":"msg:(\"SQL update\" or \"SQL select\") and file:\"DB\"","analyze_wildcard":true}}}`),
			tableInfo: &gateModel,
			parsedCfg: emptyCfgWithQuery(&queries.UnknownClause{}),
		},
		{
			descr: "fetch conditions from search query",
			request: []byte(`{"query":{"query_string":{"query":"message:(\"SQL update\" or \"SQL select\") and file:\"DB\"","analyze_wildcard":true}}}`),
			tableInfo: &gateModel,
			parsedCfg: emptyCfgWithQuery(
				queries.NewMatchQueryClause(
					`message:(\"SQL update\" or \"SQL select\") and file:\"DB\`,
					true,
					&gateModel,
				),
			),
		},
	}

	for i, test := range tests {
		parsedCfg, err := ParseElasticJSON(test.request, test.tableInfo)

		asserts := assert.New(t)

		if test.err {
			asserts.Error(err)
			continue
		} else {
			asserts.NoError(err)
		}

		asserts.Equal(test.parsedCfg.DocValueFields, parsedCfg.DocValueFields, caseName(i, test.descr, "DocValueFields"))
		asserts.Equal(test.parsedCfg.Index, parsedCfg.Index, caseName(i, test.descr, "Index"))
		asserts.Equal(test.parsedCfg.Size, parsedCfg.Size, caseName(i, test.descr, "Size"))
		if !asserts.True(queries.IsEqual(&test.parsedCfg.Sorting, &parsedCfg.Sorting), caseName(i, test.descr, "Sorting")) {
			asserts.Equal(test.parsedCfg.Sorting.String(), parsedCfg.Sorting.String(), caseName(i, test.descr, "Sorting detailed"))
		}
		compareSliceContent(asserts, test.parsedCfg.SortingFields, parsedCfg.SortingFields, caseName(i, test.descr, "SortingFields"))

		if !asserts.True(queries.IsEqual(test.parsedCfg.Query, parsedCfg.Query), caseName(i, test.descr, "Query")) {
			asserts.Equal(test.parsedCfg.Query.String(), parsedCfg.Query.String(), caseName(i, test.descr, "Query detailed"))
		}
	}
}