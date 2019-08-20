package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"bytes"
	"kibouse/config"
	"kibouse/data/models"
	"kibouse/index"
	"strconv"
)

var tableName string
var logsTopic string
var indexTopic string

// InvertedIndexRecord specifies a one-word-record for inverted index.
type InvertedIndexRecord struct {
	TS     uint64 `json:"ts"`
	Word   string `json:"word"`
	Column string `json:"column"`
}

type сonsumerGroupHandler struct {
	gateLogsSearchableFields []string
	producer                 sarama.AsyncProducer
}

func (сonsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (сonsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h сonsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)

		records, err := prepareSearchWords(msg.Value, h.gateLogsSearchableFields)
		if err != nil {
			log.Println(err.Error())
			continue
		}

		// produce to inverted index
		for _, record := range records {
			recordString, err := json.Marshal(record)
			if err != nil {
				return err
			}

			h.producer.Input() <- &sarama.ProducerMessage{
				Topic: indexTopic,
				Key:   sarama.StringEncoder(record.TS),
				Value: sarama.StringEncoder(recordString),
			}
		}

		sess.MarkMessage(msg, "")
	}
	return nil
}

func init() {
	RootCmd.AddCommand(indexerCmd)

	indexerCmd.PersistentFlags().StringVar(&tableName, "table_name", "logs_2p_gate", "Database table with logs to index")
	indexerCmd.PersistentFlags().StringVar(&logsTopic, "logs_topic", "logs_2p_gate", "Topic with logs to index")
	indexerCmd.PersistentFlags().StringVar(&indexTopic, "index_topic", "inverted_index_logs_2p_gate", "Topic with index data")
}

// indexerCmd represents the logs indexer command.
var indexerCmd = &cobra.Command{
	Use:   "indexer",
	Short: "Index logs",
	Long:  "Split logs by words and pass it further to kafka, clickhouse will form inverted index based on this data",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("indexer started")

		// load app config
		cfg, err := config.Load(cfgFile)
		if err != nil {
			log.Fatal(err.Error())
		}

		searchableFields := getLogsTableSearchableFields(tableName)
		if len(searchableFields) == 0 {
			log.Fatal("indexed table not exists or doesn't contain searchable fields")
		}

		settings := sarama.NewConfig()
		settings.Version = sarama.V1_0_0_0
		settings.Consumer.Return.Errors = true
		settings.Producer.Return.Errors = true

		// create a client
		client, err := sarama.NewClient([]string{cfg.GetKafkaSource()}, settings)
		if err != nil {
			log.Fatal(err.Error())
		}
		defer func() { _ = client.Close() }()

		// create a new consumer group
		consumer, err := sarama.NewConsumerGroupFromClient("indexer-"+logsTopic, client)
		if err != nil {
			log.Fatal(err.Error())
		}
		defer func() { _ = consumer.Close() }()

		// track errors
		go func() {
			for err := range consumer.Errors() {
				fmt.Println("ERROR", err)
			}
		}()

		// create an async producer
		producer, err := sarama.NewAsyncProducer([]string{cfg.GetKafkaSource()}, settings)
		if err != nil {
			log.Fatal(err.Error())
		}
		defer func() { _ = producer.Close() }()

		// iterate over consumer sessions
		ctx := context.Background()
		handler := сonsumerGroupHandler{gateLogsSearchableFields: searchableFields, producer: producer}
		for {
			if err := consumer.Consume(ctx, []string{logsTopic}, handler); err != nil {
				panic(err)
			}
		}
	},
}

// prepare inverted index records.
func prepareSearchWords(source []byte, searchableFields []string) ([]InvertedIndexRecord, error) {
	event := make(map[string]interface{})
	dec := json.NewDecoder(bytes.NewBuffer(source))
	dec.UseNumber()
	if err := dec.Decode(&event); err != nil {
		return nil, err
	}

	ts, ok := event["ts"]
	if !ok {
		return nil, errors.New("ts is not set")
	}

	timestamp, err := strconv.ParseUint(string(ts.(json.Number)), 10, 64)
	if err != nil {
		return nil, err
	}

	var records []InvertedIndexRecord

	for _, searchableField := range searchableFields {
		if columnValue, ok := event[searchableField]; ok {
			words := index.GetTokens(fmt.Sprintf("%s", columnValue))

			for _, word := range words {
				records = append(records, InvertedIndexRecord{
					TS:     timestamp,
					Word:   word,
					Column: searchableField,
				})
			}
		}
	}

	return records, nil
}

func getLogsTableSearchableFields(name string) []string {
	logsModels := models.GetLogsTablesSchemas()
	if logTableModel, ok := logsModels[name]; ok {
		return models.GetIndexedDbColumns(logTableModel)
	}
	return nil
}
