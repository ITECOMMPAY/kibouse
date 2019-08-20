package responses

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/pkg/errors"

	"kibouse/data/wrappers"
	"kibouse/adapter/requests/aggregations"
)

type hit struct {
	Index   string                           `json:"_index"`
	Version int                              `json:"_version"`
	ID      string                           `json:"_id"`
	Type    string                           `json:"_type"`
	Score   int                              `json:"_score"`
	Found   bool                             `json:"found"`
	Source  interface{}                      `json:"_source"`
	Sort    *sortingSectionMarshaller        `json:"sort,omitempty"`
	Fields  *docValueFieldsSectionMarshaller `json:"fields,omitempty"`
	Debug   map[string]string                `json:"_debug"`
}

type shardsStat struct {
	Total      uint `json:"total"`
	Successful uint `json:"successful"`
	Skipped    uint `json:"skipped"`
	Failed     uint `json:"failed"`
}

type Builder interface {
	CreateElasticJSON() (string, error)
	AddIndex(string)
	AddSorting([]string)
	AddDocValueFields([]string)
	AddHits(wrappers.ChDataWrapper)
	AddAggregationResult(data *aggregations.BucketAggregationData)
	AppendDebug(string, string)
}

type ResponseInputs struct {
	index          string
	sorting        []string
	docValueFields []string
	rows           wrappers.ChDataWrapper
	aggregation    *aggregations.BucketAggregationData
	debug          map[string]string
}

func (ri *ResponseInputs) AddIndex(index string) {
	ri.index = index
}

func (ri *ResponseInputs) AddSorting(sorting []string) {
	ri.sorting = sorting
}

func (ri *ResponseInputs) AddDocValueFields(docValueFields []string) {
	ri.docValueFields = docValueFields
}

func (ri *ResponseInputs) AddHits(rows wrappers.ChDataWrapper) {
	ri.rows = rows
}

func (ri *ResponseInputs) AddAggregationResult(aggregation *aggregations.BucketAggregationData) {
	ri.aggregation = aggregation
}

func (ri *ResponseInputs) AppendDebug(key string, value string) {
	if ri.debug == nil {
		ri.debug = make(map[string]string)
	}
	ri.debug[key] = value
}

type sortingSectionMarshaller struct {
	values []FieldData
}

func (ss *sortingSectionMarshaller) MarshalJSON() ([]byte, error) {
	if len(ss.values) > 0 {
		switch ss.values[0].value.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			sorting := make([]int64, len(ss.values))
			for i, sortVal := range ss.values {
				sorting[i] = sortVal.value.Int()
			}
			return json.Marshal(sorting)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			sorting := make([]uint64, len(ss.values))
			for i, sortVal := range ss.values {
				sorting[i] = sortVal.value.Uint()
			}
			return json.Marshal(sorting)
		case reflect.Float32, reflect.Float64:
			sorting := make([]float64, len(ss.values))
			for i, sortVal := range ss.values {
				sorting[i] = sortVal.value.Float()
			}
			return json.Marshal(sorting)
		case reflect.String:
			sorting := make([]string, len(ss.values))
			for i, sortVal := range ss.values {
				sorting[i] = sortVal.value.String()
			}
			return json.Marshal(sorting)
		default:
			return nil, errors.New("unsupported data type of sort values")
		}
	}
	return json.Marshal(ss.values)
}

func printFormattedValues(fieldType string, fieldVal *reflect.Value) string {
	switch fieldType {
	case "String", "DateTime":
		return fmt.Sprintf(`"%v"`, fieldVal)
	case "Date":
		date := fieldVal.Interface().(time.Time)
		return fmt.Sprintf(`"%s"`, date.Format("2006-01-02"))
	case "Timestamp":
		timestamp := time.Unix(0, int64(fieldVal.Uint()))
		return fmt.Sprintf(`"%v"`, timestamp)
	default:
		return fmt.Sprintf("%v", fieldVal.Interface())
	}
}

type docValueFieldsSectionMarshaller struct {
	fields []string
	values []FieldData
}

func (dm *docValueFieldsSectionMarshaller) MarshalJSON() ([]byte, error) {
	if len(dm.values) != len(dm.fields) {
		return nil, errors.New("doc value section couldn't be create: input arrays size mismatch")
	}
	docValueFieldsJson := make([]string, len(dm.fields))
	for i := range dm.fields {
		docValueFieldsJson[i] = fmt.Sprintf(`"%s": [%s]`, dm.fields[i], printFormattedValues(dm.values[i].dataType, dm.values[i].value))
	}
	return []byte(fmt.Sprintf("{%s}", strings.Join(docValueFieldsJson, ","))), nil
}
