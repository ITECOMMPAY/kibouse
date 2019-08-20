package models

import (
	"reflect"
	"time"
)

const GateLogsName = "logs_2p_gate"

type GateLogs struct {
	UUID                 uint64    `db:"uuid" json:"uuid" type:"UInt64" uuid:"true" mv_transform:"cityHash64(uuid)" base_type:"String"`
	Day                  time.Time `db:"day" json:"day" type:"Date" partitioning:"true" mv_transform:"today()"`
	TS                   uint64    `db:"ts" json:"ts" type:"UInt64" timestamp:"true" ch_index_pos:"1"`
	TsOriginal           string    `db:"ts_original" json:"ts_original" type:"String"`
	Type                 string    `db:"type" json:"type" type:"String"`
	Pid                  uint64    `db:"pid" json:"pid" type:"UInt64"`
	RemoteIP             string    `db:"remote_ip" json:"remote_ip" type:"String" default:""`
	Hostname             string    `db:"hostname" json:"hostname" type:"String"`
	Status               string    `db:"status" json:"status" type:"String"`
	File                 string    `db:"file" json:"file" type:"String" inv_index:"true"`
	Line                 uint16    `db:"line" json:"line" type:"UInt16"`
	Message              string    `db:"message" json:"message" type:"String" inv_index:"true"`
	LoggerID             string    `db:"logger_id" json:"logger_id" type:"String" default:""`
	IsBusinessLog        uint64    `db:"is_business_log" json:"is_business_log" type:"UInt64" default:"0"`
	PhpExecutionLoggerID string    `db:"php_execution_logger_id" json:"php_execution_logger_id" type:"String" default:"" inv_index:"true"`
	JobLoggerID          string    `db:"job_logger_id" json:"job_logger_id" type:"String" default:"" inv_index:"true"`
	SpanID               string    `db:"span_id" json:"span_id" type:"String" default:""`
	ParesEncoded         string    `db:"pares_encoded" json:"pares_encoded" type:"String" default:""`
	Pares                string    `db:"pares" json:"pares" type:"String" default:""`
	ParesXML             string    `db:"pares_xml" json:"pares_xml" type:"String" default:""`
	Error                string    `db:"error" json:"error" type:"String" default:""`
	ErrorType            string    `db:"error_type" json:"error_type" type:"String" default:""`
	Source               string    `db:"source" json:"source" type:"String" inv_index:"true"`
	Offset               string    `db:"offset" json:"offset" type:"UInt64"`
	Table                string    `db:"_table" type:"String" json:"table" skip:"db"`
}

func init() {
	models[GateLogsName] = reflect.TypeOf(GateLogs{})
}
