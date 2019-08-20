package responses

import (
	"encoding/json"
	"reflect"

	"kibouse/data/wrappers"
	"kibouse/adapter/requests/aggregations"
)

type allHits struct {
	Total    int   `json:"total"`
	MaxScore int   `json:"max_score"`
	Hits     []hit `json:"hits"`
}

type fullElasticResponse struct {
	TimedOut bool                                 `json:"timed_out"`
	Took     int                                  `json:"took"`
	Shards   shardsStat                           `json:"_shards"`
	Hits     allHits                              `json:"hits"`
	Aggs     *aggregations.BucketAggregationData  `json:"aggregations,omitempty"`
	Status   int                                  `json:"status"`
	Debug    map[string]string                    `json:"_debug"`
}

type elasticResponsesList struct {
	Responses []fullElasticResponse `json:"responses"`
}

func getNewResponseTemplate() fullElasticResponse {
	return fullElasticResponse{
		TimedOut: false,
		Took:     1,
		Shards: shardsStat{
			Total:      5,
			Successful: 5,
			Skipped:    0,
			Failed:     0,
		},
		Hits: allHits{
			Total: 0,
		},
		Aggs:   nil,
		Status: 200,
	}
}

type all struct {
	multiple      bool
	responsesList elasticResponsesList
	ResponseInputs
}

// CreateElasticJSON converts result into JSON string compatible with kibana.
func (f *all) CreateElasticJSON() (string, error) {
	response := getNewResponseTemplate()
	response.Aggs = f.aggregation

	if f.rows != nil {
		response.Hits.Hits = make([]hit, 0, f.rows.Items())
		f.rows.Reset()

		for item := f.rows.NextItem(); item != nil; item = f.rows.NextItem(){
			response.Hits.Total++

			sortSection := fetchFieldValuesByName(f.sorting, item)
			docValsSection := fetchFieldValuesByName(f.docValueFields, item)
			response.Hits.Hits = append(response.Hits.Hits, hit{
				Index:   f.index,
				Type:    item.ChTableName(),
				Version: 1,
				ID:      item.ID(),
				Score:   1,
				Found:   true,
				Source:  item.Data(),
				Sort:    &sortingSectionMarshaller{values: sortSection},
				Fields:  &docValueFieldsSectionMarshaller{values: docValsSection, fields: f.docValueFields},
			})
		}

	} else if f.aggregation != nil {
		response.Hits.Total = int(f.aggregation.DocCount())
	}

	response.Debug = f.debug

	var bytes []byte
	var err error = nil

	if f.multiple {
		f.responsesList.Responses = append(f.responsesList.Responses, response)
		bytes, err = json.Marshal(f.responsesList)
	} else {
		bytes, err = json.Marshal(response)
	}

	if err != nil {
		return "", err
	}

	return string(bytes), nil
}

func NewFullResponseBuilder(multiple bool) Builder {
	if multiple {
		responsesList := elasticResponsesList{
			Responses: make([]fullElasticResponse, 0, 1),
		}
		return &all{multiple: multiple, responsesList: responsesList}
	}
	return &all{multiple: multiple}
}

type FieldData struct {
	value    *reflect.Value
	dataType string
}

func fetchFieldValuesByName(fields []string, item wrappers.DataItem) []FieldData {
	fieldValues := make([]FieldData, 0)
	for _, field := range fields {
		if fieldVal, ok := item.AttrValue(field); ok {
			fieldValues = append(
				fieldValues,
				FieldData{value: fieldVal, dataType: item.ModelScheme().DataFields[field].CHType},
			)
		}
	}
	return fieldValues
}