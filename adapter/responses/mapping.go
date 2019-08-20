package responses

import (
	"encoding/json"

	"kibouse/data/models"
)

func CreateFieldCapsJSON(tableInfo map[string]*models.FieldProps) ([]byte, error) {
	jsonStruct := createBaseMappingJson()
	for name, clkInfo := range tableInfo {
		elasticType := clickhouseTypeToElastic(clkInfo.CHType)
		elcInfo := info{Type: elasticType, Searchable: true}
		if elasticType != "text" {
			elcInfo.Aggregatable = true
		}
		addFieldMappingJson(name, elcInfo, jsonStruct)
	}
	return json.Marshal(&jsonStruct)
}

type info struct {
	Type       string `json:"type"`
	Searchable bool   `json:"searchable"`
	Aggregatable bool `json:"aggregatable"`
}

type mapping struct {
	Fields map[string]map[string]info `json:"fields"`
}

func createBaseMappingJson() *mapping {
	jsonMapping := mapping{
		Fields: make(map[string]map[string]info),
	}
	addFieldMappingJson("_index", info{Type: "_index", Searchable: true, Aggregatable: true}, &jsonMapping)
	addFieldMappingJson("_type", info{Type: "_type", Searchable: true, Aggregatable: true}, &jsonMapping)
	addFieldMappingJson("_source", info{Type: "_source", Searchable: false, Aggregatable: false}, &jsonMapping)
	addFieldMappingJson("_id", info{Type: "_id", Searchable: true, Aggregatable: true}, &jsonMapping)
	return &jsonMapping
}

func addFieldMappingJson(name string, params info, json *mapping) {
	json.Fields[name] = make(map[string]info)
	json.Fields[name][params.Type] = params
}

// clickhouseTypeToElastic converts clickhouse data types to elastic.
func clickhouseTypeToElastic(clickhouseType string) string {
	switch clickhouseType {
	case "String", "Array(String)":
		return "text"
	case "Bool":
		return "boolean"
	case "UInt16", "Array(UInt16)", "UInt32", "Array(UInt32)", "UInt64", "Array(UInt64)", "Int8", "Array(Int8)",
		"Int16", "Array(Int16)", "Int32", "Array(Int32)", "Int64", "Array(Int64)":
		return "long"
	case "Float32", "Array(Float32)", "Float64", "Array(Float64)":
		return "float"
	case "Timestamp", "Date", "DateTime":
		return "date"
	default:
		return "keyword"
	}
}