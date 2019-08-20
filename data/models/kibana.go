package models

import "time"

const SettingsTableName = "kibana"

type ClickhouseSettings struct {
	Table                  string    `db:"_table" type:"String" json:"_table" skip:"db"`
	ID                     string    `db:"_id" type:"String" ch_index_pos:"1"`
	Type                   string    `db:"type" type:"String"`
	UpdatedAt              time.Time `db:"updated_at" type:"DateTime"`
	Description            string    `db:"description" type:"String" default:""`
	Hits                   uint32    `db:"hits" type:"UInt32" default:"0"`
	SearchSourceJSON       string    `db:"searchSourceJSON" type:"String" default:""`
	Title                  string    `db:"title" type:"String" default:""`
	Version                int32     `db:"version" type:"Int32" default:"1"`
	UiStateJSON            string    `db:"uiStateJSON" type:"String" default:""`
	BuildNum               uint32    `db:"buildNum" type:"UInt32" default:"0"`
	DefaultIndex           string    `db:"defaultIndex" type:"String" default:""`
	OptionsJSON            string    `db:"optionsJSON" type:"String" default:""`
	PanelsJSON             string    `db:"panelsJSON" type:"String" default:""`
	RefreshIntervalDisplay string    `db:"refreshInterval_display" type:"String" default:""`
	RefreshIntervalPause   uint8     `db:"refreshInterval_pause" type:"UInt8" default:"0"`
	RefreshIntervalSection int32     `db:"refreshInterval_section" type:"Int32" default:"0"`
	RefreshIntervalValue   int32     `db:"refreshInterval_value" type:"Int32" default:"0"`
	TimeFrom               string    `db:"timeFrom" type:"String" default:""`
	TimeRestore            uint8     `db:"timeRestore" type:"UInt8" default:"0"`
	TimeTo                 string    `db:"timeTo" type:"String" default:""`
	NumLinks               int32     `db:"numLinks" type:"Int32" default:"0"`
	NumVertices            int32     `db:"numVertices" type:"Int32" default:"0"`
	WsState                string    `db:"wsState" type:"String" default:""`
	FieldFormatMap         string    `db:"fieldFormatMap" type:"String" default:""`
	Fields                 string    `db:"fields" type:"String" default:""`
	IntervalName           string    `db:"intervalName" type:"String" default:""`
	NotExpandable          uint8     `db:"notExpandable" type:"UInt8" default:"0"`
	SourceFilters          string    `db:"sourceFilters" type:"String" default:""`
	TimeFieldName          string    `db:"timeFieldName" type:"String" default:""`
	SearchColumns          []string  `db:"columns" type:"Array(String)" default:"array()"`
	SearchSort             []string  `db:"sort" type:"Array(String)" default:"array()"`
	ServerUuid             string    `db:"uuid" type:"String" default:""`
	TimelionChartHeight    int32     `db:"timelion_chart_height" type:"Int32" default:"0"`
	TimelionColumns        int32     `db:"timelion_columns" type:"Int32" default:"0"`
	TimelionInterval       string    `db:"timelion_interval" type:"String" default:""`
	TimelionOtherInterval  string    `db:"timelion_other_interval" type:"String" default:""`
	TimelionRows           int32     `db:"timelion_rows" type:"Int32" default:"0"`
	TimelionSheet          string    `db:"timelion_sheet" type:"String" default:""`
	AccessCount            int64     `db:"accessCount" type:"Int64" default:"0"`
	AccessDate             time.Time `db:"accessDate" type:"DateTime" default:"toDateTime('1970-01-01 00:00:00')"`
	CreateDate             time.Time `db:"createDate" type:"DateTime" default:"toDateTime('1970-01-01 00:00:00')"`
	Url                    string    `db:"url" type:"String" default:""`
	SavedSearchId          string    `db:"savedSearchId" type:"String" default:""`
	VisState               string    `db:"visState" type:"String" default:""`
	Sign                   int8      `db:"sign" type:"Int8" sign:"true"`
}

func NewClickhouseSettings(id string, entryType string) *ClickhouseSettings {
	return &ClickhouseSettings{
		ID:        id,
		Type:      entryType,
		Table:     SettingsTableName,
		UpdatedAt: time.Now(),
		Sign:      1,
	}
}
