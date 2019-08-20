package clickhouse

import (
	"reflect"
	"testing"
	"time"
)

type responsesMapping struct {
	Url      string `db:"url" type:"String"`
	Response string `db:"response" type:"String"`
}

func TestCreateLogsTableScheme(t *testing.T) {
	testData := []struct {
		caseName  string
		dbName    string
		tableName string
		data      reflect.Type
		result    string
	}{
		{
			caseName:  "creating predefined responses table scheme",
			dbName:    "logs",
			tableName: "responses",
			data:      reflect.TypeOf(responsesMapping{}),
			result:    "CREATE TABLE IF NOT EXISTS logs.responses (url String, response String)ENGINE = Log;",
		},
	}

	for _, test := range testData {
		scheme := CreateLogsTableScheme(test.tableName, test.data)
		result, _ := scheme.BuildScheme(test.dbName)
		if result != test.result {
			t.Error(
				"For", test.caseName,
				"\n expected: ", test.result,
				"\n got: ", result,
			)
		}
	}
}

type clickhouseSettings struct {
	Day                                time.Time `db:"day" json:"day" type:"Date" default:"today()" partitioning:"true"`
	Table                              string    `db:"_table" type:"String" json:"_table" skip:"db"`
	Type                               string    `db:"type" type:"String" ch_index_pos:"2"`
	ID                                 string    `db:"_id" type:"String" ch_index_pos:"1"`
	UpdatedAt                          time.Time `db:"updated_at" type:"DateTime"`
	Description                        string    `db:"description" type:"String" default:""`
	Hits                               uint32    `db:"hits" type:"UInt32" default:"0"`
	SearchSourceJSON                   string    `db:"searchSourceJSON" type:"String" default:""`
	Title                              string    `db:"title" type:"String" default:""`
	Version                            int32     `db:"version" type:"Int32" default:"1"`
	UiStateJSON                        string    `db:"uiStateJSON" type:"String" default:""`
	ConfigBuildNum                     uint32    `db:"config_buildNum" type:"UInt32" default:"0"`
	ConfigDefaultIndex                 string    `db:"config_defaultIndex" type:"String" default:""`
	DashboardOptionsJSON               string    `db:"dashboard_optionsJSON" type:"String" default:""`
	DashboardPanelsJSON                string    `db:"dashboard_panelsJSON" type:"String" default:""`
	DashboardRefreshIntervalDisplay    string    `db:"dashboard_refreshInterval_display" type:"String" default:""`
	DashboardRefreshIntervalPause      uint8     `db:"dashboard_refreshInterval_pause" type:"UInt8" default:"0"`
	DashboardRefreshIntervalSection    int32     `db:"dashboard_refreshInterval_section" type:"Int32" default:"0"`
	DashboardRefreshIntervalValue      int32     `db:"dashboard_refreshInterval_value" type:"Int32" default:"0"`
	DashboardTimeFrom                  uint64    `db:"dashboard_timeFrom" type:"UInt64" default:"0"`
	DashboardTimeRestore               uint8     `db:"dashboard_timeRestore" type:"UInt8" default:"0"`
	DashboardTimeTo                    uint64    `db:"dashboard_timeTo" type:"UInt64" default:"0"`
	GraphWorkspaceNumLinks             int32     `db:"graph_workspace_numLinks" type:"Int32" default:"0"`
	GraphWorkspaceNumVertices          int32     `db:"graph_workspace_numVertices" type:"Int32" default:"0"`
	GraphWorkspaceWsState              string    `db:"graph_workspace_wsState" type:"String" default:""`
	IndexPatternFieldFormatMap         string    `db:"index_pattern_fieldFormatMap" type:"String" default:""`
	IndexPatternFields                 string    `db:"index_pattern_fields" type:"String" default:""`
	IndexPatternIntervalName           string    `db:"index_pattern_intervalName" type:"String" default:""`
	IndexPatternNotExpandable          uint8     `db:"index_pattern_notExpandable" type:"UInt8" default:"0"`
	IndexPatternSourceFilters          []string  `db:"index_pattern_sourceFilters" type:"Array(String)" default:"array()"`
	IndexPatternTimeFieldName          string    `db:"index_pattern_timeFieldName" type:"String" default:""`
	SearchColumns                      []string  `db:"search_columns" type:"Array(String)" default:"array()"`
	SearchSort                         []string  `db:"search_sort" type:"Array(String)" default:"array()"`
	ServerUuid                         string    `db:"server_uuid" type:"String" default:""`
	TimelionSheetTimelionChartHeight   int32     `db:"timelion_sheet_timelion_chart_height" type:"Int32" default:"0"`
	TimelionSheetTimelionColumns       int32     `db:"timelion_sheet_timelion_columns" type:"Int32" default:"0"`
	TimelionSheetTimelionInterval      string    `db:"timelion_sheet_timelion_interval" type:"String" default:""`
	TimelionSheetTimelionOtherInterval string    `db:"timelion_sheet_timelion_other_interval" type:"String" default:""`
	TimelionSheetTimelionRows          int32     `db:"timelion_sheet_timelion_rows" type:"Int32" default:"0"`
	TimelionSheetTimelionSheet         string    `db:"timelion_sheet_timelion_sheet" type:"String" default:""`
	UrlAccessCount                     int64     `db:"url_accessCount" type:"Int64" default:"0"`
	UrlAccessDate                      time.Time `db:"url_accessDate" type:"DateTime" default:"toDateTime('1970-01-01 00:00:00')"`
	UrlCreateDate                      time.Time `db:"url_createDate" type:"DateTime" default:"toDateTime('1970-01-01 00:00:00')"`
	Url                                string    `db:"url" type:"String" default:""`
	VisualizationSavedSearchId         string    `db:"visualization_savedSearchId" type:"String" default:""`
	VisualizationVisState              string    `db:"visualization_visState" type:"String" default:""`
}

type gateLogs struct {
	Day        time.Time `db:"day" json:"day" type:"Date" default:"today()" partitioning:"true"`
	UUID       string    `db:"uuid" json:"uuid" type:"String"`
	TS         uint64    `db:"ts" json:"ts" type:"UInt64" timestamp:"true" ch_index_pos:"1"`
	TsOriginal string    `db:"ts_original" json:"ts_original" type:"String"`
	Type       string    `db:"type" json:"type" type:"String" ch_index_pos:"2"`
	Pid        uint64    `db:"pid" json:"pid" type:"UInt64"`
	RemoteIP   string    `db:"remote_ip" json:"remote_ip" type:"String" default:""`

	Hostname string `db:"hostname" json:"hostname" type:"String"`
	Status   string `db:"status" json:"status" type:"String"`
	File     string `db:"file" json:"file" type:"String"`
	Line     uint16 `db:"line" json:"line" type:"UInt16"`
	Message  string `db:"message" json:"message" type:"String"`

	LoggerID             string `db:"logger_id" json:"logger_id" type:"String" default:""`
	IsBusinessLog        uint8  `db:"is_business_log" json:"is_business_log" type:"UInt8" default:"0"`
	PhpExecutionLoggerID string `db:"php_execution_logger_id" json:"php_execution_logger_id" type:"String" default:""`
	JobLoggerID          string `db:"job_logger_id" json:"job_logger_id" type:"String" default:""`
	SpanID               string `db:"span_id" json:"span_id" type:"String" default:""`
	ParesEncoded         string `db:"pares_encoded" json:"pares_encoded" type:"String" default:""`
	Pares                string `db:"pares" json:"pares" type:"String" default:""`
	ParesXML             string `db:"pares_xml" json:"pares_xml" type:"String" default:""`
	Error                string `db:"error" json:"error" type:"String" default:""`
	ErrorType            string `db:"error_type" json:"error_type" type:"String" default:""`

	Source string `db:"source" json:"source" type:"String"`

	Table string `db:"_table" type:"String" json:"_table" skip:"db"`
}

type invertedIndex struct {
	Hash   uint64    `db:"word_hash" type:"UInt64" ch_index_pos:"1"`
	TS     uint64    `db:"ts" type:"UInt64" timestamp:"true" ch_index_pos:"2"`
	UUID   string    `db:"uuid" type:"String"`
	Column uint64    `db:"column_hash" type:"UInt64" ch_index_pos:"3"`
	Day    time.Time `db:"day" type:"Date" partitioning:"true"`
}

type clickhouseTables struct {
	Day   time.Time `db:"day" json:"day" type:"Date" default:"today()" partitioning:"true"`
	Index string    `db:"_index" type:"String" ch_index_pos:"1"`
	Table string    `db:"_table" json:"_table" type:"String" skip:"db"`
}

type histogramPreparedTable struct {
	Day   time.Time `db:"day" type:"Date" partitioning:"true"`
	Count int64     `db:"count" type:"UInt64"`
	Key   int64     `db:"key" type:"Int64" ch_index_pos:"1"`
}

func TestCreateMergeTreeTablesScheme(t *testing.T) {
	testData := []struct {
		dbName      string
		tableName   string
		granularity uint
		data        reflect.Type
		result      string
	}{
		{
			dbName:      "logs",
			tableName:   "tables",
			granularity: DefaultIndexGranularity,
			data:        reflect.TypeOf(clickhouseTables{}),
			result:      "CREATE TABLE IF NOT EXISTS logs.tables (day Date DEFAULT today(), _index String)ENGINE = MergeTree() PARTITION BY day ORDER BY (_index) SETTINGS index_granularity=8192;",
		},
		{
			dbName:      "data",
			tableName:   "logs_2p_gate",
			granularity: DefaultIndexGranularity,
			data:        reflect.TypeOf(gateLogs{}),
			result:      "CREATE TABLE IF NOT EXISTS data.logs_2p_gate (day Date DEFAULT today(), uuid String, ts UInt64, ts_original String, type String, pid UInt64, remote_ip String DEFAULT '', hostname String, status String, file String, line UInt16, message String, logger_id String DEFAULT '', is_business_log UInt8 DEFAULT 0, php_execution_logger_id String DEFAULT '', job_logger_id String DEFAULT '', span_id String DEFAULT '', pares_encoded String DEFAULT '', pares String DEFAULT '', pares_xml String DEFAULT '', error String DEFAULT '', error_type String DEFAULT '', source String)ENGINE = MergeTree() PARTITION BY day ORDER BY (ts,type) SETTINGS index_granularity=8192;",
		},
	}

	for i, test := range testData {
		scheme := CreateMergeTreeTableScheme(test.tableName, test.data, test.granularity)
		result, _ := scheme.BuildScheme(test.dbName)
		if result != test.result {
			t.Error(
				"For step:", i,
				"\n expected: ", test.result,
				"\n got: ", result,
			)
		}
	}
}

func TestCreateCollapsingMergeTreeTableScheme(t *testing.T) {
	testData := []struct {
		dbName      string
		tableName   string
		granularity uint
		data        reflect.Type
		sign        string
		result      string
	}{
		{
			dbName: "logs",
			tableName: "kibana",
			granularity: DefaultIndexGranularity,
			data: reflect.TypeOf(clickhouseSettings{}),
			sign: "sign",
			result: "CREATE TABLE IF NOT EXISTS logs.kibana (day Date DEFAULT today(), type String, _id String, updated_at DateTime, description String DEFAULT '', hits UInt32 DEFAULT 0, searchSourceJSON String DEFAULT '', title String DEFAULT '', version Int32 DEFAULT 1, uiStateJSON String DEFAULT '', config_buildNum UInt32 DEFAULT 0, config_defaultIndex String DEFAULT '', dashboard_optionsJSON String DEFAULT '', dashboard_panelsJSON String DEFAULT '', dashboard_refreshInterval_display String DEFAULT '', dashboard_refreshInterval_pause UInt8 DEFAULT 0, dashboard_refreshInterval_section Int32 DEFAULT 0, dashboard_refreshInterval_value Int32 DEFAULT 0, dashboard_timeFrom UInt64 DEFAULT 0, dashboard_timeRestore UInt8 DEFAULT 0, dashboard_timeTo UInt64 DEFAULT 0, graph_workspace_numLinks Int32 DEFAULT 0, graph_workspace_numVertices Int32 DEFAULT 0, graph_workspace_wsState String DEFAULT '', index_pattern_fieldFormatMap String DEFAULT '', index_pattern_fields String DEFAULT '', index_pattern_intervalName String DEFAULT '', index_pattern_notExpandable UInt8 DEFAULT 0, index_pattern_sourceFilters Array(String) DEFAULT array(), index_pattern_timeFieldName String DEFAULT '', search_columns Array(String) DEFAULT array(), search_sort Array(String) DEFAULT array(), server_uuid String DEFAULT '', timelion_sheet_timelion_chart_height Int32 DEFAULT 0, timelion_sheet_timelion_columns Int32 DEFAULT 0, timelion_sheet_timelion_interval String DEFAULT '', timelion_sheet_timelion_other_interval String DEFAULT '', timelion_sheet_timelion_rows Int32 DEFAULT 0, timelion_sheet_timelion_sheet String DEFAULT '', url_accessCount Int64 DEFAULT 0, url_accessDate DateTime DEFAULT toDateTime('1970-01-01 00:00:00'), url_createDate DateTime DEFAULT toDateTime('1970-01-01 00:00:00'), url String DEFAULT '', visualization_savedSearchId String DEFAULT '', visualization_visState String DEFAULT '')ENGINE = CollapsingMergeTree(sign) PARTITION BY day ORDER BY (_id,type) SETTINGS index_granularity=8192;",
		},
	}

	for i, test := range testData {
		scheme := CreateCollapsingMergeTreeTableScheme(test.tableName, test.data, test.sign, test.granularity)
		result, _ := scheme.BuildScheme(test.dbName)
		if result != test.result {
			t.Error(
				"For step:", i,
				"\n expected: ", test.result,
				"\n got: ", result,
			)
		}
	}
}

func TestCreateKafkaTableScheme(t *testing.T) {
	testData := []struct {
		dbName          string
		tableName         string
		kafkaBrokerList   string
		kafkaTopicList    string
		kafkaGroupName    string
		kafkaFormat       string
		kafkaRowDelimiter string
		kafkaSchema       string
		kafkaNumConsumers uint
		data              reflect.Type
		result            string
	}{
		{
			dbName:            "logs",
			tableName:         "queue_2p_gate",
			kafkaBrokerList:   "kafka01.test:9092",
			kafkaTopicList:    "logs_2p_gate",
			kafkaGroupName:    "logs_2p_gate",
			kafkaFormat:       "JSONEachRow",
			kafkaRowDelimiter: `\0`,
			kafkaSchema:       "",
			kafkaNumConsumers: 1,
			data:              reflect.TypeOf(gateLogs{}),
			result:            `CREATE TABLE IF NOT EXISTS logs.queue_2p_gate (uuid String, ts UInt64, ts_original String, type String, pid UInt64, remote_ip String DEFAULT '', hostname String, status String, file String, line UInt16, message String, logger_id String DEFAULT '', is_business_log UInt8 DEFAULT 0, php_execution_logger_id String DEFAULT '', job_logger_id String DEFAULT '', span_id String DEFAULT '', pares_encoded String DEFAULT '', pares String DEFAULT '', pares_xml String DEFAULT '', error String DEFAULT '', error_type String DEFAULT '', source String)ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka01.test:9092', kafka_topic_list = 'logs_2p_gate', kafka_group_name = 'logs_2p_gate', kafka_format = 'JSONEachRow', kafka_row_delimiter = '\0', kafka_schema = '', kafka_num_consumers = 1;`,
		},
	}

	for i, test := range testData {
		scheme := CreateKafkaTableScheme(
			test.tableName,
			test.data,
			test.kafkaBrokerList,
			test.kafkaTopicList,
			test.kafkaGroupName,
			test.kafkaFormat,
			test.kafkaRowDelimiter,
			test.kafkaSchema,
			test.kafkaNumConsumers,
		)
		result, _ := scheme.BuildScheme(test.dbName)
		if result != test.result {
			t.Error(
				"For step:", i,
				"\n expected: ", test.result,
				"\n got: ", result,
			)
		}
	}
}

func TestCreateMatViewScheme(t *testing.T) {
	testData := []struct {
		dbName  string
		tableName string
		from      string
		to        string
		data      reflect.Type
		result    string
	}{
		{
			dbName:    "data",
			tableName: "consumer_2p_gate",
			from:      "queue_2p_gate",
			to:        "logs_2p_gate",
			data:      reflect.TypeOf(gateLogs{}),
			result:    `CREATE MATERIALIZED VIEW IF NOT EXISTS data.consumer_2p_gate TO data.logs_2p_gate (day Date DEFAULT today(), uuid String, ts UInt64, ts_original String, type String, pid UInt64, remote_ip String DEFAULT '', hostname String, status String, file String, line UInt16, message String, logger_id String DEFAULT '', is_business_log UInt8 DEFAULT 0, php_execution_logger_id String DEFAULT '', job_logger_id String DEFAULT '', span_id String DEFAULT '', pares_encoded String DEFAULT '', pares String DEFAULT '', pares_xml String DEFAULT '', error String DEFAULT '', error_type String DEFAULT '', source String) AS SELECT day, uuid, ts, ts_original, type, pid, remote_ip, hostname, status, file, line, message, logger_id, is_business_log, php_execution_logger_id, job_logger_id, span_id, pares_encoded, pares, pares_xml, error, error_type, source FROM data.queue_2p_gate`,
		},
	}

	for i, test := range testData {
		scheme := CreateMatViewScheme(test.tableName, test.data, test.from, test.to)
		result, _ := scheme.BuildScheme(test.dbName)
		if result != test.result {
			t.Error(
				"For step: ", i,
				"\n expected: ", test.result,
				"\n got: ", result,
			)
		}
	}
}

func TestCreateDataTransformMatViewScheme(t *testing.T) {
	testData := []struct {
		dbName     string
		tableName    string
		from         string
		to           string
		data         reflect.Type
		transformDef string
		groupBy      string
		result       string
	}{
		{
			dbName:       "data",
			tableName:    "consumer_inverted_index_2p_gate",
			from:         "queue_inverted_index_2p_gate",
			to:           "sharded_inverted_index_logs_2p_gate",
			data:         reflect.TypeOf(invertedIndex{}),
			transformDef: "cityHash64(word) as word_hash, ts, uuid, cityHash64(column) as column_hash, toDate(toDateTime(intDiv(ts, 1000000000))) AS day",
			result:       "CREATE MATERIALIZED VIEW IF NOT EXISTS data.consumer_inverted_index_2p_gate TO data.sharded_inverted_index_logs_2p_gate (word_hash UInt64, ts UInt64, uuid String, column_hash UInt64, day Date) AS SELECT cityHash64(word) as word_hash, ts, uuid, cityHash64(column) as column_hash, toDate(toDateTime(intDiv(ts, 1000000000))) AS day FROM data.queue_inverted_index_2p_gate",
		},
		{
			dbName:       "kibouse",
			tableName:    "consumer_inverted_index_2p_gate",
			from:         "queue_inverted_index_2p_gate",
			to:           "sharded_inverted_index_logs_2p_gate",
			data:         reflect.TypeOf(histogramPreparedTable{}),
			transformDef: "today() AS day, toInt64((ts) / 300000000000) as key, count() as count",
			groupBy:      "day, key",
			result:       "CREATE MATERIALIZED VIEW IF NOT EXISTS kibouse.consumer_inverted_index_2p_gate TO kibouse.sharded_inverted_index_logs_2p_gate (day Date, count UInt64, key Int64) AS SELECT today() AS day, toInt64((ts) / 300000000000) as key, count() as count FROM kibouse.queue_inverted_index_2p_gate GROUP BY day, key",
		},
	}

	for i, test := range testData {
		scheme := CreateDataTransformMatViewScheme(test.tableName, test.data, test.from, test.to, test.transformDef, test.groupBy)
		result, _ := scheme.BuildScheme(test.dbName)
		if result != test.result {
			t.Error(
				"For step: ", i,
				"\n expected: ", test.result,
				"\n got: ", result,
			)
		}
	}
}
