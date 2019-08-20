package wrappers

import (
	"reflect"

	"github.com/pkg/errors"

	"kibouse/data/models"
	"kibouse/adapter/settings"
)

type KibanaSettings struct {
	dataContainer
	data []models.ClickhouseSettings
}

func init() {
	factories[models.SettingsTableName] = NewKibanaSettings
}

// NewKibanaSettings creates data container for kibana settings
func NewKibanaSettings() (ChDataWrapper, error) {
	dbFieldsMapping, err := models.CreateDBFieldsInfoMap(reflect.TypeOf(models.ClickhouseSettings{}))
	if err != nil {
		return nil, err
	}
	return &KibanaSettings{
		dataContainer: dataContainer{
			index: 0,
			modelInfo: models.ModelInfo{
				DBName:     models.SettingsTableName,
				DataFields: dbFieldsMapping,
			},
		},
		data: nil,
	}, nil
}

type KibanaSettingsItem struct {
	id string
	data *settings.ElasticSettings
	modelInfo *models.ModelInfo
}

func (i KibanaSettingsItem) ID() string {
	return i.id
}

func (i KibanaSettingsItem) Data() interface{} {
	return i.data
}

func (i KibanaSettingsItem) AttrValue(name string) (*reflect.Value, bool) {
	if sourceName, ok := i.modelInfo.ClickhouseAttrCodeName(name); ok {
		return getValueByName(sourceName, i.data)
	}
	return nil, false
}

func (i KibanaSettingsItem) ChTableName() string {
	return models.SettingsTableName
}

func (i KibanaSettingsItem) ModelScheme() *models.ModelInfo {
	return i.modelInfo
}

func (container *KibanaSettings) NextClickhouseSettings() *models.ClickhouseSettings {
	if container == nil {
		return nil
	}
	if container.index < len(container.data) {
		container.index++
		return &container.data[container.index-1]
	}
	return nil
}

func (container *KibanaSettings) NextItem() DataItem {
	if container == nil {
		return nil
	}
	if clickhouseSettings := container.NextClickhouseSettings(); clickhouseSettings != nil {
		return KibanaSettingsItem {
			data: settings.ClickhouseToElastic(clickhouseSettings),
			modelInfo: &container.modelInfo,
			id: clickhouseSettings.ID,
		}
	}
	return nil
}

func (container *KibanaSettings) FetchData(loaderFunc loader) error {
	if container == nil {
		return errors.New("data wrapper is not initialized")
	}
	container.data = make([]models.ClickhouseSettings, 0)
	return loaderFunc(&container.data)
}

func (container *KibanaSettings) Items() int {
	if container == nil {
		return 0
	}
	return len(container.data)
}
