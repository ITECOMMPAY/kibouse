package settings

import (
	"time"

	"kibouse/data/models"
)

const DateTimeFormat = "2006-01-02T15:04:05.000Z"

func CreateElasticSettingsItem(id string, source *ElasticSettings) *ElasticSettingsItem {
	return &ElasticSettingsItem{
		Index:  "kibana",
		Type:   source.Type,
		ID:     id,
		Score:  1,
		Source: source,
	}
}

// ElasticSettingsItem represents elastic hit item with .kibana index entry.
type ElasticSettingsItem struct {
	Index  string           `json:"_index"`
	Type   string           `json:"_type"`
	ID     string           `json:"_id"`
	Score  float64          `json:"_score"`
	Source *ElasticSettings `json:"_source"`
}

// ElasticSettings contains all possible fields from .kibana index.
type ElasticSettings struct {
	Type                  string                    `json:"type" db:"type"`
	UpdatedAt             string                    `json:"updated_at"`
	FieldFormatMap        string                    `json:"fieldFormatMap,omitempty"`
	Fields                string                    `json:"fields,omitempty"`
	IntervalName          string                    `json:"intervalName,omitempty"`
	NotExpendable         bool                      `json:"notExpandable,omitempty"`
	SourceFilters         string                    `json:"sourceFilters,omitempty"`
	TimeFieldName         string                    `json:"timeFieldName,omitempty"`
	Title                 string                    `json:"title,omitempty"`
	VisState              string                    `json:"visState,omitempty"`
	UiStateJSON           string                    `json:"uiStateJSON,omitempty"`
	Description           string                    `json:"description,omitempty"`
	Version               int32                     `json:"version,omitempty"`
	SavedSearchId         string                    `json:"savedSearchId,omitempty"`
	KibanaSavedObjectMeta KibanaSavedObjectMetaData `json:"kibanaSavedObjectMeta,omitempty"`
	Hits                  uint32                    `json:"hits,omitempty"`
	OptionsJSON           string                    `json:"optionsJSON,omitempty"`
	PanelsJSON            string                    `json:"panelsJSON,omitempty"`
	RefreshInterval       RefreshIntervalSettings   `json:"refreshInterval,omitempty"`
	TimeFrom              string                    `json:"timeFrom,omitempty"`
	TimeRestore           bool                      `json:"timeRestore,omitempty"`
	TimeTo                string                    `json:"timeTo,omitempty"`
	AccessCount           int64                     `json:"accessCount,omitempty"`
	AccessDate            string                    `json:"accessDate,omitempty"`
	CreateDate            string                    `json:"createDate,omitempty"`
	Url                   string                    `json:"url,omitempty"`
	BuildNum              uint32                    `json:"buildNum,omitempty"`
	DefaultIndex          string                    `json:"defaultIndex,omitempty"`
	Columns               []string                  `json:"columns,omitempty"`
	Sort                  []string                  `json:"sort,omitempty"`
	TimelionChartHeight   int32                     `json:"timelion_chart_height,omitempty"`
	TimelionColumns       int32                     `json:"timelion_columns,omitempty"`
	TimelionInterval      string                    `json:"timelion_interval,omitempty"`
	TimelionOtherInterval string                    `json:"timelion_other_interval,omitempty"`
	TimelionRows          int32                     `json:"timelion_rows,omitempty"`
	TimelionSheet         string                    `json:"timelion_sheet,omitempty"`
	Uuid                  string                    `json:"uuid,omitempty"`
}

type KibanaSavedObjectMetaData struct {
	SearchSourceJSON string `json:"searchSourceJSON"`
}

type RefreshIntervalSettings struct {
	Display string `json:"display"`
	Pause   bool   `json:"pause"`
	Section int32  `json:"section"`
	Value   int32  `json:"value"`
}

type ResponseMapping struct {
	Url      string `db:"url" type:"String"`
	Response string `db:"response" type:"String"`
}

// Convert settings item from clickhouse storing format to elastic
func ClickhouseToElastic(clickhouse *models.ClickhouseSettings) *ElasticSettings {
	if clickhouse == nil {
		return nil
	}
	return &ElasticSettings{
		Type:           clickhouse.Type,
		UpdatedAt:      dateToString(clickhouse.UpdatedAt),
		FieldFormatMap: clickhouse.FieldFormatMap,
		Fields:         clickhouse.Fields,
		IntervalName:   clickhouse.IntervalName,
		NotExpendable:  clickhouse.NotExpandable > 0,
		SourceFilters:  clickhouse.SourceFilters,
		TimeFieldName:  clickhouse.TimeFieldName,
		Title:          clickhouse.Title,
		BuildNum:       clickhouse.BuildNum,
		DefaultIndex:   clickhouse.DefaultIndex,
		Description:    clickhouse.Description,
		Hits:           clickhouse.Hits,
		OptionsJSON:    clickhouse.OptionsJSON,
		PanelsJSON:     clickhouse.PanelsJSON,
		RefreshInterval: RefreshIntervalSettings{
			Display: clickhouse.RefreshIntervalDisplay,
			Pause:   clickhouse.RefreshIntervalPause > 0,
			Section: clickhouse.RefreshIntervalSection,
			Value:   clickhouse.RefreshIntervalValue,
		},
		TimeFrom:    clickhouse.TimeFrom,
		TimeRestore: clickhouse.TimeRestore > 0,
		TimeTo:      clickhouse.TimeTo,
		UiStateJSON: clickhouse.UiStateJSON,
		Version:     clickhouse.Version,
		KibanaSavedObjectMeta: KibanaSavedObjectMetaData{
			SearchSourceJSON: clickhouse.SearchSourceJSON,
		},
		VisState:              clickhouse.VisState,
		SavedSearchId:         clickhouse.SavedSearchId,
		AccessCount:           clickhouse.AccessCount,
		AccessDate:            dateToString(clickhouse.AccessDate),
		CreateDate:            dateToString(clickhouse.CreateDate),
		Url:                   clickhouse.Url,
		Columns:               clickhouse.SearchColumns,
		Sort:                  clickhouse.SearchSort,
		TimelionChartHeight:   clickhouse.TimelionChartHeight,
		TimelionColumns:       clickhouse.TimelionColumns,
		TimelionInterval:      clickhouse.TimelionInterval,
		TimelionOtherInterval: clickhouse.TimelionOtherInterval,
		TimelionRows:          clickhouse.TimelionRows,
		TimelionSheet:         clickhouse.TimelionSheet,
		Uuid:                  clickhouse.ServerUuid,
	}
}

// Convert settings item from elastic format to clickhouse
func ElasticToClickhouse(elasticCfg *ElasticSettingsItem) *models.ClickhouseSettings {
	clickhouseCfg := models.NewClickhouseSettings(elasticCfg.ID, elasticCfg.Type)

	if clickhouseCfg.Type == "config" {
		clickhouseCfg.BuildNum = elasticCfg.Source.BuildNum
		clickhouseCfg.DefaultIndex = elasticCfg.Source.DefaultIndex
	}
	if clickhouseCfg.Type == "dashboard" {
		clickhouseCfg.Description = elasticCfg.Source.Description
		clickhouseCfg.Hits = elasticCfg.Source.Hits
		clickhouseCfg.SearchSourceJSON = elasticCfg.Source.KibanaSavedObjectMeta.SearchSourceJSON
		clickhouseCfg.OptionsJSON = elasticCfg.Source.OptionsJSON
		clickhouseCfg.PanelsJSON = elasticCfg.Source.PanelsJSON
		clickhouseCfg.RefreshIntervalDisplay = elasticCfg.Source.RefreshInterval.Display
		clickhouseCfg.RefreshIntervalPause = convertToClickhouseBool(elasticCfg.Source.RefreshInterval.Pause)
		clickhouseCfg.RefreshIntervalSection = elasticCfg.Source.RefreshInterval.Section
		clickhouseCfg.RefreshIntervalValue = elasticCfg.Source.RefreshInterval.Value
		clickhouseCfg.TimeFrom = elasticCfg.Source.TimeFrom
		clickhouseCfg.TimeRestore = convertToClickhouseBool(elasticCfg.Source.TimeRestore)
		clickhouseCfg.TimeTo = elasticCfg.Source.TimeTo
		clickhouseCfg.Title = elasticCfg.Source.Title
		clickhouseCfg.UiStateJSON = elasticCfg.Source.UiStateJSON
		clickhouseCfg.Version = elasticCfg.Source.Version
	}
	if clickhouseCfg.Type == "index-pattern" {
		clickhouseCfg.FieldFormatMap = elasticCfg.Source.FieldFormatMap
		clickhouseCfg.Fields = elasticCfg.Source.Fields
		clickhouseCfg.IntervalName = elasticCfg.Source.IntervalName
		clickhouseCfg.NotExpandable = convertToClickhouseBool(elasticCfg.Source.NotExpendable)
		clickhouseCfg.SourceFilters = elasticCfg.Source.SourceFilters
		clickhouseCfg.TimeFieldName = elasticCfg.Source.TimeFieldName
		clickhouseCfg.Title = elasticCfg.Source.Title
	}
	if clickhouseCfg.Type == "search" {
		clickhouseCfg.SearchColumns = elasticCfg.Source.Columns
		clickhouseCfg.Description = elasticCfg.Source.Description
		clickhouseCfg.Hits = elasticCfg.Source.Hits
		clickhouseCfg.SearchSourceJSON = elasticCfg.Source.KibanaSavedObjectMeta.SearchSourceJSON
		clickhouseCfg.SearchSort = elasticCfg.Source.Sort
		clickhouseCfg.Title = elasticCfg.Source.Title
		clickhouseCfg.Version = elasticCfg.Source.Version
	}
	if clickhouseCfg.Type == "server" {
		clickhouseCfg.ServerUuid = elasticCfg.Source.Uuid
	}
	if clickhouseCfg.Type == "timelion-sheet" {
		clickhouseCfg.Description = elasticCfg.Source.Description
		clickhouseCfg.Hits = elasticCfg.Source.Hits
		clickhouseCfg.SearchSourceJSON = elasticCfg.Source.KibanaSavedObjectMeta.SearchSourceJSON
		clickhouseCfg.TimelionChartHeight = elasticCfg.Source.TimelionChartHeight
		clickhouseCfg.TimelionColumns = elasticCfg.Source.TimelionColumns
		clickhouseCfg.TimelionInterval = elasticCfg.Source.TimelionInterval
		clickhouseCfg.TimelionOtherInterval = elasticCfg.Source.TimelionOtherInterval
		clickhouseCfg.TimelionRows = elasticCfg.Source.TimelionRows
		clickhouseCfg.TimelionSheet = elasticCfg.Source.TimelionSheet
		clickhouseCfg.Title = elasticCfg.Source.Title
		clickhouseCfg.Version = elasticCfg.Source.Version
	}
	if clickhouseCfg.Type == "url" {
		clickhouseCfg.AccessCount = elasticCfg.Source.AccessCount
		clickhouseCfg.AccessDate, _ = stringToDate(elasticCfg.Source.AccessDate)
		clickhouseCfg.CreateDate, _ = stringToDate(elasticCfg.Source.CreateDate)
		clickhouseCfg.Url = elasticCfg.Source.Url
	}
	if clickhouseCfg.Type == "visualization" {
		clickhouseCfg.Description = elasticCfg.Source.Description
		clickhouseCfg.SearchSourceJSON = elasticCfg.Source.KibanaSavedObjectMeta.SearchSourceJSON
		clickhouseCfg.SavedSearchId = elasticCfg.Source.SavedSearchId
		clickhouseCfg.Title = elasticCfg.Source.Title
		clickhouseCfg.UiStateJSON = elasticCfg.Source.UiStateJSON
		clickhouseCfg.Version = elasticCfg.Source.Version
		clickhouseCfg.VisState = elasticCfg.Source.VisState
	}
	return clickhouseCfg
}

func convertToClickhouseBool(val bool) uint8 {
	if val == true {
		return 1
	} else {
		return 0
	}
}

func dateToString(t time.Time) string {
	return t.Format(DateTimeFormat)
}

func stringToDate(date string) (time.Time, error) {
	t, err := time.Parse(DateTimeFormat, date)
	if err != nil {
		return time.Now(), err
	}
	return t, nil
}
