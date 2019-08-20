package responses

import (
	"reflect"
	"testing"
	"time"

	"kibouse/data/models"
)

type Sports2 struct {
	UUID      string    `db:"uuid" json:"@uuid" type:"String"`
	Day       time.Time `db:"day" json:"day" type:"Date"`
	TimeStamp uint64    `db:"timestamp" json:"timestamp" type:"UInt64" timestamp:"true"`
	Name      string    `db:"Name" json:"Name" type:"String"`
	Birthdate string    `db:"birthdate" json:"birthdate" type:"String"`
	Sport     string    `db:"sport" json:"sport" type:"String"`
	Rating    []string  `db:"rating" json:"rating" type:"Array(String)"`
	Location  string    `db:"location" json:"location" type:"String"`
	TestInt   int32     `db:"test_int" json:"test_int" type:"Array(Int32)"`
	TestFloat float64   `db:"test_float" json:"test_float" type:"Array(Float64)"`
	Table     string    `db:"_table" json:"_table" type:"String"`
}

func TestCreateElasticMapping(t *testing.T) {
	testData := []struct {
		caseName string
		data     reflect.Type
		result   string
	}{
		{
			caseName: "kibana settings db fields information",
			data:     reflect.TypeOf(Sports2{}),
			result:   `{"fields":{"_id":{"_id":{"type":"_id","searchable":true,"aggregatable":true}},"_index":{"_index":{"type":"_index","searchable":true,"aggregatable":true}},"_source":{"_source":{"type":"_source","searchable":false,"aggregatable":false}},"_table":{"text":{"type":"text","searchable":true,"aggregatable":false}},"_type":{"_type":{"type":"_type","searchable":true,"aggregatable":true}},"birthdate":{"text":{"type":"text","searchable":true,"aggregatable":false}},"day":{"date":{"type":"date","searchable":true,"aggregatable":true}},"location":{"text":{"type":"text","searchable":true,"aggregatable":false}},"Name":{"text":{"type":"text","searchable":true,"aggregatable":false}},"rating":{"text":{"type":"text","searchable":true,"aggregatable":false}},"sport":{"text":{"type":"text","searchable":true,"aggregatable":false}},"test_float":{"float":{"type":"float","searchable":true,"aggregatable":true}},"test_int":{"long":{"type":"long","searchable":true,"aggregatable":true}},"timestamp":{"date":{"type":"date","searchable":true,"aggregatable":true}},"uuid":{"text":{"type":"text","searchable":true,"aggregatable":false}}}}`,
		},
	}

	for _, test := range testData {
		mapping, _ := models.CreateDBFieldsInfoMap(test.data)
		result, _ := CreateFieldCapsJSON(mapping)
		if string(result) != test.result {
			t.Error(
				"For", test.caseName,
				"\n expected: ", test.result,
				"\n got: ", string(result),
			)
		}
	}
}
