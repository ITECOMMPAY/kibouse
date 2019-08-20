package models

import (
	"fmt"
	"reflect"
	"strings"
)

const timeStampType = "Timestamp"

var models = map[string]reflect.Type{}

// GetLogsTablesSchemas returns names and types of models used for storing logs.
func GetLogsTablesSchemas() map[string]reflect.Type {
	return models
}

// FieldProps contains information about ClickHouse representation of data field.
type CHField struct {
	CHName string
	CHType string
}

func (f CHField) IsArray() bool {
	return strings.HasPrefix(f.CHType, "Array")
}

func (f CHField) GetBaseChType() string {
	return strings.Trim(strings.TrimPrefix(f.CHType, "Array"), "()")
}

func (f CHField) IsString() bool {
	fieldType := f.GetBaseChType()
	switch fieldType {
	case "String":
		return true
	}
	return false
}

func (f CHField) IsNumeric() bool {
	fieldType := f.GetBaseChType()
	switch fieldType {
	case "UInt8", "UInt16", "UInt32", "UInt64", "Int8", "Int16", "Int32", "Int64", "Float32", "Float64", "Decimal":
		return true
	}
	return false
}

// FieldProps contains information about storing and representation of single logs record property.
type FieldProps struct {
	CHField
	SourceCodeName string
	KibanaName     string
	IsUUID         bool
	FullTextSearch bool
}

// ModelInfo contains logs storage information.
type ModelInfo struct {
	DBName     string
	DataFields map[string]*FieldProps
}

// GetTimestampField returns properties of model timestamp attribute.
func (mi ModelInfo) GetTimestampField() (*FieldProps, bool) {
	for i := range mi.DataFields {
		if mi.DataFields[i].CHType == timeStampType {
			return mi.DataFields[i], true
		}
	}
	return nil, false
}

// GetTimestampField returns properties of model id attribute.
func (mi ModelInfo) GetUuidField() (*FieldProps, bool) {
	for i := range mi.DataFields {
		if mi.DataFields[i].IsUUID == true {
			return mi.DataFields[i], true
		}
	}
	return nil, false
}

// ClickhouseAttrCodeName converts clickhouse attribute name to its corresponding source code variable name.
func (mi ModelInfo) ClickhouseAttrCodeName(attr string) (string, bool) {
	if info, ok := mi.DataFields[attr]; ok {
		return info.SourceCodeName, true
	}
	return "", false
}

// GetStructureTags fetch values of required tag from all fields of structure.
func GetStructureTags(t reflect.Type, tagName string, skipPartField bool) (tags []string, err error) {
	defer func() {
		if r := recover(); r != nil {
			tags = nil
			err = fmt.Errorf("panic in GetStructureTags: %v", r)
		}
	}()

	tags = make([]string, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if tag, found := field.Tag.Lookup(tagName); found {
			_, isPartField := field.Tag.Lookup("partitioning")
			if !(field.Tag.Get("skip") == tagName || (isPartField && skipPartField)) {
				tags = append(tags, tag)
			}
		}
	}
	return tags, nil
}

// CreateDBFieldsInfoMap used for generating db fields mapping to its types and internal names.
func CreateDBFieldsInfoMap(t reflect.Type) (mapping map[string]*FieldProps, err error) {
	defer func() {
		if r := recover(); r != nil {
			mapping = nil
			err = fmt.Errorf("panic in CreateDBFieldsInfoMap: %v", r)
		}
	}()
	mapping = make(map[string]*FieldProps)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tags := getFieldTags(field)
		if tags.CHName == "" {
			continue
		}
		if tags.CHType == "Uint8" {
			tags.CHType = "Bool"
		}
		_, isTime := field.Tag.Lookup("timestamp")
		if isTime {
			tags.CHType = timeStampType
		}
		tags.SourceCodeName = field.Name
		mapping[tags.CHName] = tags
	}

	return mapping, nil
}

// InvertedIndexRequired checks that model contains text fields which requires full text search.
func InvertedIndexRequired(t reflect.Type) (res bool) {
	defer func() {
		if r := recover(); r != nil {
			res = false
		}
	}()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if value, ok := field.Tag.Lookup("inv_index"); ok && value == "true" {
			return true
		}
	}
	return false
}

func getFieldTags(field reflect.StructField) (tags *FieldProps) {
	tags = &FieldProps{}
	tags.CHName = field.Tag.Get("db")
	tags.KibanaName = field.Tag.Get("json")
	tags.CHType = field.Tag.Get("type")
	if value, ok := field.Tag.Lookup("inv_index"); ok && value == "true" {
		tags.FullTextSearch = true
	}
	if value, ok := field.Tag.Lookup("uuid"); ok && value == "true" {
		tags.IsUUID = true
	}
	return
}

// GetIndexedDbColumns returns list of database table columns with indexed content.
func GetIndexedDbColumns(model reflect.Type) (res []string) {
	defer func() {
		if r := recover(); r != nil {
			res = nil
		}
	}()
	res = make([]string, 0, model.NumField())
	for i := 0; i < model.NumField(); i++ {
		tags := getFieldTags(model.Field(i))
		if tags.CHName != "" && tags.FullTextSearch {
			res = append(res, tags.CHName)
		}
	}
	return res
}

