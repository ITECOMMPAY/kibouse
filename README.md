# Clickhouse adapter for Kibana

Kibouse is a Go tool that can be used to provide supporting **[Kibana](https://github.com/elastic/kibana)** as analytics and search dashboard for **[Clickhouse](https://github.com/yandex/ClickHouse)**

## Configuration

Kibouse loads its configuration from the $KIBOUSE_HOME/config/config.yaml by default.

Here is the default kibouse configuration file:

```yaml
app:
  # default kibouse port, kibana should be set up to it before the first run.
  listening_port: "8888"
  # reset kibana configuration (e.g. remove index patterns, visualizations, dashboards) at the startup.
  reset: true
  # kibouse predefined responses to kibana static requests.
  static_responses: "../config/static_responses.json"
  # kibana version.
  kibana_ver: "5.6.8"
  # create clickhouse tables for logs delivery at the startup. 
  create_ch_tables: true

  logging:
    # debug messages logging.
    log_debug_messages: true
    # logging all http messages between kibana and kibouse.
    log_requests: true
    # file for logging all http transactions between kibana and kibouse.
    log_requests_file: "../http_transactions.log"
    # kibouse works only as reverse proxy to elastic. 
    proxy_to_elastic: false

  sources:
    # clickhouse client address  
    clickhouse: "tcp://127.0.0.1:9000"
    # kafka address for indexer
    kafka: "kafka.test:9092"
    # elasticsearch address (only for reverse proxy mode)
    elasticsearch: "http://localhost:9200"
```

## Usage

### Before the first run:

1. Create blank logs entity model with its data accessing wrapper.

```bash
./kibouse/autogen/autogen -c=<clickhouse logs table> -s=<logs source code structure> [-d=<path to kibouse/data folder>]
```
2. Update entity model according to the actual log structure. 

example: 

```go
type GateLogs struct {
	UUID                 uint64    `db:"uuid" json:"uuid" type:"UInt64" uuid:"true" ch_index_pos:"2" mv_transform:"cityHash64(uuid)" base_type:"String"`
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
```
### supported field tags
required:

db - clickhouse attribute name

json - elasticsearch parameter name

type - clickhouse attribute type

uuid - field contains record id

inv_index - full text search supporting required for this field 

optional (uses only to autonatically create CH tables at kibouse startup, not required when Clickhouse tables already exist):

ch_index_pos - sets attribute as the part of CH index

partitioning - partitioning key

default - default attribute value in CH


3. Build kibouse

```bash
cd kibouse
make build
```

4. Update kibana configuration file (kibana.yml), set elasticsearch.url to kibouse address and listening port.

5. Set correct kibana version in kibouse/config/static_responses.json to avoid compatibility warnings. 

6. Start clickhouse (client and server), kibouse and kibana

### Kibouse launch

Start kibouse adapter 

./kibouse/bin/kibouse [-config=<configuration file path>]

Start indexer tool for logs tokenization and updating inverted index. 

./kibouse/bin/kibouse indexer [-config=<configuration file path>]

## Limitations

1. supported kibana versions:

5.6.*

2. supported data aggregations(visualization page):

Top level: date histogram

Nested: filters
