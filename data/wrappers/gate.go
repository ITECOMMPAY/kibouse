package wrappers

import (
	"reflect"
	"strconv"

	"github.com/pkg/errors"

	"kibouse/data/models"
)

func init() {
	factories[models.GateLogsName] = NewGateLogsWrapper
}

// NewGateLogsWrapper returns data container for gate logs
func NewGateLogsWrapper() (ChDataWrapper, error) {
	dbFieldsMapping, err := models.CreateDBFieldsInfoMap(reflect.TypeOf(models.GateLogs{}))
	if err != nil {
		return nil, err
	}
	return &GateLogsWrapper{
		dataContainer: dataContainer{
			index: 0,
			modelInfo: models.ModelInfo{
				DBName:     models.GateLogsName,
				DataFields: dbFieldsMapping,
			},
		},
		data: nil,
	}, nil
}

type GateLogsItem struct {
	data models.GateLogs
	modelInfo *models.ModelInfo
}

func (i GateLogsItem) ID() string {
	return strconv.FormatUint(i.data.UUID, 10)
}

func (i GateLogsItem) Data() interface{} {
	return i.data
}

func (i GateLogsItem) AttrValue(name string) (*reflect.Value, bool) {
	if sourceName, ok := i.modelInfo.ClickhouseAttrCodeName(name); ok {
		return getValueByName(sourceName, i.data)
	}
	return nil, false
}

func (i GateLogsItem) ChTableName() string {
	return i.data.Table
}

func (i GateLogsItem) ModelScheme() *models.ModelInfo {
	return i.modelInfo
}

type GateLogsWrapper struct {
	dataContainer
	data []models.GateLogs
}

func (container *GateLogsWrapper) NextItem() DataItem {
	if container == nil {
		return nil
	}
	if container.index < len(container.data) {
		container.index++
		return GateLogsItem {
			data: container.data[container.index-1],
			modelInfo: &container.modelInfo,
		}
	}
	return nil
}

func (container *GateLogsWrapper) FetchData(loaderFunc loader) error {
	if container == nil {
		return errors.New("data wrapper is not initialized")
	}
	container.data = make([]models.GateLogs, 0)
	return loaderFunc(&container.data)
}

func (container *GateLogsWrapper) Items() int {
	if container == nil {
		return 0
	}
	return len(container.data)
}
