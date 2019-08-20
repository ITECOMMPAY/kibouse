package wrappers

import (
	"reflect"

	"kibouse/data/models"
)

type loader func(interface{}) error


// DataItem is the common interface for accessing model entry data.
type DataItem interface {
	ID() string
	Data() interface{}
	AttrValue(string) (*reflect.Value, bool)
	ChTableName() string
	ModelScheme() *models.ModelInfo
}

// ChDataWrapper common interface for accessing clickhouse data.
type ChDataWrapper interface {
	FetchData(loader) error
	Reset()
	Items() int
	NextItem() DataItem
	ModelScheme() *models.ModelInfo
}

// getValueByName returns field value by its name from arbitrary structure
func getValueByName(fieldName string, container interface{}) (*reflect.Value, bool) {
	r := reflect.ValueOf(container)
	value := reflect.Indirect(r).FieldByName(fieldName)
	if !value.IsValid() {
		return nil, false
	}
	return &value, true
}

type dataContainer struct {
	index     int
	modelInfo models.ModelInfo
}

// Reset the internal index of current log entry
func (container *dataContainer) Reset() {
	if container == nil {
		return
	}
	container.index = 0
}

// ModelScheme returns database names to internal names mapping
func (container *dataContainer) ModelScheme() *models.ModelInfo {
	if container == nil {
		return nil
	}
	return &container.modelInfo
}