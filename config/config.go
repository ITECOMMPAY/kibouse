package config

import (
	"encoding/json"
	"os"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type sources struct {
	clickhouse    string
	elasticsearch string
	kafka         string
}

type logging struct {
	logDebugMessages bool
	logRequests      bool
	elasticOnly      bool
	httpTransLogFile string
}

// AppConfig contains application settings
type AppConfig struct {
	listeningPort string
	resetRequired bool
	responses     map[string]string
	kibanaVer     string
	createChTables bool
	sources       *sources
	logging       *logging
}

const (
	DefaultConfigName = "config"
	DefaultConfigType = "yaml"

	HttpTransactionsLogFile = "http_transactions.log"

	StaticResponsesFile = "static_responses.json"
	KibanaVersion       = "5.6.8"
)

// Load reads settings from file to AppConfig structure
func Load(path string) (*AppConfig, error) {
	if path != "" {
		viper.SetConfigFile(path)
	} else {
		viper.AddConfigPath(".")
		viper.SetConfigName(DefaultConfigName)
		viper.SetConfigType(DefaultConfigType)
	}

	viper.SetDefault("app.listening_port", "8888")
	viper.SetDefault("app.reset", true)
	viper.SetDefault("app.static_responses", StaticResponsesFile)
	viper.SetDefault("app.kibana_ver", KibanaVersion)
	viper.SetDefault("app.create_ch_tables", false)

	viper.SetDefault("app.sources.clickhouse", "tcp://127.0.0.1:9000")
	viper.SetDefault("app.sources.elasticsearch", "http://localhost:9200")
	viper.SetDefault("app.sources.kafka", "kafka01.test:9092")

	viper.SetDefault("app.logging.log_debug_messages", true)
	viper.SetDefault("app.logging.log_requests", false)
	viper.SetDefault("app.logging.proxy_to_elastic", false)
	viper.SetDefault("app.logging.log_requests_file", HttpTransactionsLogFile)

	if err := viper.ReadInConfig(); err != nil {
		return nil, errors.New("cannot parse config file - " + err.Error())
	}

	staticResponses, err := readStaticRespones(viper.GetString("app.static_responses"))
	if err != nil {
		return nil, err
	}

	config := &AppConfig{
		listeningPort: viper.GetString("app.listening_port"),
		resetRequired: viper.GetBool("app.reset"),
		responses:     staticResponses,
		kibanaVer:     viper.GetString("app.kibana_ver"),
		createChTables: viper.GetBool("app.create_ch_tables"),

		sources: &sources{
			clickhouse:    viper.GetString("app.sources.clickhouse"),
			elasticsearch: viper.GetString("app.sources.elasticsearch"),
			kafka:         viper.GetString("app.sources.kafka"),
		},
		logging: &logging{
			logDebugMessages: viper.GetBool("app.logging.log_debug_messages"),
			logRequests:      viper.GetBool("app.logging.log_requests"),
			elasticOnly:      viper.GetBool("app.logging.proxy_to_elastic"),
			httpTransLogFile: viper.GetString("app.logging.log_requests_file"),
		},
	}

	return config, nil
}

func (cfg *AppConfig) GetListeningPort() string {
	return cfg.listeningPort
}

func (cfg *AppConfig) GetClickhouseSource() string {
	return cfg.sources.clickhouse
}

func (cfg *AppConfig) GetElasticSource() string {
	return cfg.sources.elasticsearch
}

func (cfg *AppConfig) GetKafkaSource() string {
	return cfg.sources.kafka
}

func (cfg *AppConfig) LogRequests() bool {
	return cfg.logging.logRequests
}

func (cfg *AppConfig) ProxyToElastic() bool {
	return cfg.logging.elasticOnly
}

func (cfg *AppConfig) LogDebugMessages() bool {
	return cfg.logging.logDebugMessages
}

func (cfg *AppConfig) ReinitRequired() bool {
	return cfg.resetRequired
}

func (cfg *AppConfig) HttpTransLogFile() string {
	return cfg.logging.httpTransLogFile
}

func (cfg *AppConfig) StaticResponse(url string) (string, bool) {
	response, ok := cfg.responses[url]
	return response, ok
}

func (cfg *AppConfig) KibanaVersion() string {
	return cfg.kibanaVer
}

func (cfg *AppConfig) CreateChTables() bool {
	return cfg.createChTables
}

func readStaticRespones(path string) (map[string]string, error) {
	staticResponses := make(map[string]string)

	file, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrap(err, "cannot open file with elasticsearch static responses: %s")
	}

	decoder := json.NewDecoder(file)
	if err = decoder.Decode(&staticResponses); err != nil {
		return nil, errors.Wrap(err, "cannot decode elasticsearch static responses to json")
	}

	return staticResponses, nil
}
