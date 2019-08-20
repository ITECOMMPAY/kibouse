package clickhouse

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/pkg/errors"

	"kibouse/data/models"
	"kibouse/db"
)

type EngineType uint

const (
	Kafka           EngineType = 0
	MatView         EngineType = 1
	MergeTreeFamily EngineType = 2

	DefaultIndexGranularity uint = 8192

	StreamerPrefix = "queue_"
	ConsumerPrefix = "consumer_"
)

// CreateLogsTableScheme creates scheme for adding new table with RuntimeLog engine
func CreateLogsTableScheme(name string, dataStructure reflect.Type) db.Scheme {
	return &logsTableScheme{
		schemeBase: schemeBase{
			name:          name,
			dataStructure: dataStructure,
		},
	}
}

func newBaseMergeTreeScheme(name string, dataStructure reflect.Type, indGranularity uint) *mergeTreeTableScheme {
	return &mergeTreeTableScheme{
		schemeBase: schemeBase{
			name:          name,
			dataStructure: dataStructure,
		},
		indGranularity: indGranularity,
	}
}

// CreateMergeTreeTableScheme creates scheme for adding new table with MergeTree family engines
func CreateMergeTreeTableScheme(name string, dataStructure reflect.Type, indGranularity uint) db.Scheme {
	scheme := newBaseMergeTreeScheme(name, dataStructure, indGranularity)
	scheme.getEngineTypeDefinition = func() string {
		return "ENGINE = MergeTree() "
	}
	return scheme
}

// CreateCollapsingMergeTreeTableScheme creates scheme for adding new table with CollapsingMergeTree family engines
func CreateCollapsingMergeTreeTableScheme(name string, dataStructure reflect.Type, sign string, indGranularity uint) db.Scheme {
	scheme := newBaseMergeTreeScheme(name, dataStructure, indGranularity)
	scheme.getEngineTypeDefinition = func() string {
		return fmt.Sprintf("ENGINE = CollapsingMergeTree(%s) ", sign)
	}
	return scheme
}

// CreateSummingMergeTreeTableScheme creates scheme for adding new table with SummingMergeTree family engines
func CreateSummingMergeTreeTableScheme(name string, dataStructure reflect.Type, indGranularity uint) db.Scheme {
	scheme := newBaseMergeTreeScheme(name, dataStructure, indGranularity)
	scheme.getEngineTypeDefinition = func() string {
		return "ENGINE = SummingMergeTree() "
	}
	return scheme
}

// CreateKafkaTableScheme creates scheme for adding new table with Kafka engines
func CreateKafkaTableScheme(name string,
	dataStructure reflect.Type,
	kafkaBrokerList string,
	kafkaTopicList string,
	kafkaGroupName string,
	kafkaFormat string,
	kafkaRowDelimiter string,
	kafkaSchema string,
	kafkaNumConsumers uint) db.Scheme {
	return &kafkaTableScheme{
		schemeBase: schemeBase{
			name:          name,
			dataStructure: dataStructure,
		},
		kafkaBrokerList:   kafkaBrokerList,
		kafkaTopicList:    kafkaTopicList,
		kafkaGroupName:    kafkaGroupName,
		kafkaFormat:       kafkaFormat,
		kafkaRowDelimiter: kafkaRowDelimiter,
		kafkaSchema:       kafkaSchema,
		kafkaNumConsumers: kafkaNumConsumers,
	}
}

// CreateMatViewScheme creates scheme for adding new materialized view.
func CreateMatViewScheme(name string, dataStructure reflect.Type, from string, to string) db.Scheme {
	return &matViewScheme{
		schemeBase: schemeBase{
			name:          name,
			dataStructure: dataStructure,
		},
		from: from,
		to:   to,
	}
}

// CreateDataConvertMatViewScheme creates material view with data transformation.
func CreateDataTransformMatViewScheme(name string,
	dataStructure reflect.Type,
	from string,
	to string,
	transformation string,
	groupBy string,
) db.Scheme {
	return &dataTransformMatViewScheme{
		matViewScheme: matViewScheme{
			schemeBase: schemeBase{
				name:          name,
				dataStructure: dataStructure,
			},
			from: from,
			to:   to,
		},
		transformDef: transformation,
		groupBy:      groupBy,
	}
}

// CreateDataDeliveryQueue creates tables for storing data delivered via kafka.
func CreateDataDeliveryQueue(name string, kafkaTopic string, dataStruct reflect.Type, kafka string) error {
	if err := CreateLogsDeliveryQueue(name, kafkaTopic, dataStruct, kafka); err != nil {
		return err
	}

	if err := CreateHistogramPreCalcQueue(name, kafkaTopic, dataStruct, kafka); err != nil {
		return err
	}

	if models.InvertedIndexRequired(dataStruct) {
		if err := CreateLogsIndexingQueue(name, kafkaTopic, kafka); err != nil {
			return err
		}
	}

	return nil
}

// CreateDataDeliveryQueue creates tables for storing log entries delivered via kafka.
func CreateLogsDeliveryQueue(name string, kafkaTopic string, dataStruct reflect.Type, kafka string) error {
	if err := db.CreateTable(
		CreateMergeTreeTableScheme(
			name, dataStruct, DefaultIndexGranularity),
	); err != nil {
		return err
	}
	if err := db.CreateTable(
		CreateKafkaTableScheme(StreamerPrefix+name,
			dataStruct,
			kafka,
			kafkaTopic,
			name,
			"JSONEachRow",
			`\0`,
			"",
			1),
	); err != nil {
		return err
	}
	if err := db.CreateTable(
		CreateMatViewScheme(
			ConsumerPrefix+name,
			dataStruct,
			StreamerPrefix+name,
			name),
	); err != nil {
		return err
	}

	return nil
}

// CreateLogsIndexingQueue uses for creating data delivery queue for logs indexing.
func CreateLogsIndexingQueue(indexingTable string, kafkaTopic string, kafka string) error {
	if err := db.CreateTable(
		CreateMergeTreeTableScheme(
			models.InvertedIndexTablePrefix+indexingTable,
			reflect.TypeOf(models.InvertedIndex{}),
			DefaultIndexGranularity,
		),
	); err != nil {
		return err
	}
	if err := db.CreateTable(
		CreateKafkaTableScheme(StreamerPrefix+models.InvertedIndexTablePrefix+indexingTable,
			reflect.TypeOf(models.IndexingQueueItem{}),
			kafka,
			models.InvertedIndexTablePrefix+kafkaTopic,
			models.InvertedIndexTablePrefix+indexingTable,
			"JSONEachRow",
			`\0`,
			"",
			1),
	); err != nil {
		return err
	}
	if err := db.CreateTable(
		CreateDataTransformMatViewScheme(
			ConsumerPrefix+models.InvertedIndexTablePrefix+indexingTable,
			reflect.TypeOf(models.InvertedIndex{}),
			StreamerPrefix+models.InvertedIndexTablePrefix+indexingTable,
			models.InvertedIndexTablePrefix+indexingTable,
			"toDate(toDateTime(intDiv(ts, 1000000000))) AS day, ts, cityHash64(word) as word_hash, cityHash64(column) as column_hash",
			""),
	); err != nil {
		return err
	}

	return nil
}

// CreateHistogramPreCalcQueue uses for creating data delivery queue for histogram pre calculation.
func CreateHistogramPreCalcQueue(logsTable string, kafkaTopic string, dataStruct reflect.Type, kafka string) error {
	if err := db.CreateTable(
		CreateKafkaTableScheme(StreamerPrefix+models.PreparedHistogramDataTablePrefix+logsTable,
			dataStruct,
			kafka,
			kafkaTopic,
			models.PreparedHistogramDataTablePrefix+logsTable,
			"JSONEachRow",
			`\0`,
			"",
			1),
	); err != nil {
		return err
	}
	if err := db.CreateTable(
		CreateSummingMergeTreeTableScheme(
			models.PreparedHistogramDataTablePrefix+logsTable,
			reflect.TypeOf(models.HistogramPreCalcTable{}),
			DefaultIndexGranularity,
		),
	); err != nil {
		return err
	}
	if err := db.CreateTable(
		CreateDataTransformMatViewScheme(
			ConsumerPrefix+models.PreparedHistogramDataTablePrefix+logsTable,
			reflect.TypeOf(models.HistogramPreCalcTable{}),
			StreamerPrefix+models.PreparedHistogramDataTablePrefix+logsTable,
			models.PreparedHistogramDataTablePrefix+logsTable,
			"today() AS day, toInt64((ts) / 300000000000) as key, count() as count",
			"day, key"),
	); err != nil {
		return err
	}
	return nil
}

type schemeBase struct {
	name          string
	dataStructure reflect.Type
}

type logsTableScheme struct {
	schemeBase
}

func (lts *logsTableScheme) BuildScheme(dbName string) (string, error) {
	return buildScheme(lts.dataStructure,
		tableHead(dbName+"."+lts.name),
		func(t reflect.Type) string {
			return fullFieldsDefinition(t, true, MergeTreeFamily)
		},
		func(data reflect.Type) string {
			return "ENGINE = Log;"
		})
}

func buildMergeTreeIndex(fields []indexField) string {
	if len(fields) == 0 {
		return ""
	}
	sort.Slice(fields, func(i int, j int) bool {
		return fields[i].position < fields[j].position
	})
	result := ""
	for i := range fields {
		result += fields[i].name + ","
	}
	return result[:len(result)-1]
}

func buildMergeTreeEngineSettings(partitioning string, indexedFields []indexField, granularity uint) string {
	settings := strings.Builder{}
	if partitioning != "" {
		settings.WriteString(fmt.Sprintf("PARTITION BY %s ", partitioning))
	}
	if chIndex := buildMergeTreeIndex(indexedFields); chIndex != "" {
		settings.WriteString(fmt.Sprintf("ORDER BY (%s) ", chIndex))
	}
	if granularity != 0 {
		settings.WriteString(fmt.Sprintf("SETTINGS index_granularity=%d;", granularity))
	}

	return settings.String()
}

type mergeTreeTableScheme struct {
	schemeBase
	indGranularity          uint
	getEngineTypeDefinition func() string
}

type indexField struct {
	position int
	name     string
}

func (mtts *mergeTreeTableScheme) mergeTreeEngineBuilder() func(data reflect.Type) string {
	return func(data reflect.Type) string {
		indices := make([]indexField, 0, 1)

		partition := ""
		for i := 0; i < data.NumField(); i++ {
			field := data.Field(i)
			if pos, isIndex := field.Tag.Lookup("ch_index_pos"); isIndex {
				res, err := strconv.Atoi(pos)
				if err == nil {
					indices = append(indices, indexField{res, field.Tag.Get("db")})
				}
			}
			if _, isPart := field.Tag.Lookup("partitioning"); isPart {
				partition = field.Tag.Get("db")
			}
		}

		return mtts.getEngineTypeDefinition() +
			buildMergeTreeEngineSettings(partition, indices, mtts.indGranularity)
	}
}

func (mtts *mergeTreeTableScheme) BuildScheme(dbName string) (string, error) {
	return buildScheme(mtts.dataStructure,
		tableHead(dbName+"."+mtts.name),
		func(t reflect.Type) string {
			return fullFieldsDefinition(t, false, MergeTreeFamily)
		},
		mtts.mergeTreeEngineBuilder(),
	)
}

const kafkaEngineTpl = "ENGINE = Kafka " +
	"SETTINGS " +
	"kafka_broker_list = '%s', " +
	"kafka_topic_list = '%s', " +
	"kafka_group_name = '%s', " +
	"kafka_format = '%s', " +
	"kafka_row_delimiter = '%s', " +
	"kafka_schema = '%s', " +
	"kafka_num_consumers = %d;"


type kafkaTableScheme struct {
	schemeBase
	kafkaBrokerList   string
	kafkaTopicList    string
	kafkaGroupName    string
	kafkaFormat       string
	kafkaRowDelimiter string
	kafkaSchema       string
	kafkaNumConsumers uint
}

func (kts *kafkaTableScheme) BuildScheme(dbName string) (string, error) {
	return buildScheme(kts.dataStructure,
		tableHead(dbName+"."+kts.name),
		func(t reflect.Type) string {
			return fullFieldsDefinition(t, true, Kafka)
		},
		func(data reflect.Type) string {
			return fmt.Sprintf(kafkaEngineTpl,
				kts.kafkaBrokerList,
				kts.kafkaTopicList,
				kts.kafkaGroupName,
				kts.kafkaFormat,
				kts.kafkaRowDelimiter,
				kts.kafkaSchema,
				kts.kafkaNumConsumers)
		})
}

type matViewScheme struct {
	schemeBase
	from string
	to   string
}

func (mvs *matViewScheme) mvHead(db string) string {
	convertedDataScheme := fullFieldsDefinition(mvs.dataStructure, false, MatView)
	return 	fmt.Sprintf(
		"CREATE MATERIALIZED VIEW IF NOT EXISTS %s.%s TO %s.%s %s AS SELECT ",
		db,
		mvs.name,
		db,
		mvs.to,
		convertedDataScheme,
	)
}

func (mvs *matViewScheme) BuildScheme(dbName string) (string, error) {
	return buildScheme(mvs.dataStructure,
		mvs.mvHead(dbName),
		func(data reflect.Type) string {
			exprs, err := getDbFieldsPreInsertTransformation(data)
			if err != nil {
				panic("cannot create db fields list")
			}
			return strings.Join(exprs, ", ")
		},
		func(data reflect.Type) string {
			return fmt.Sprintf(" FROM %s.%s", dbName, mvs.from)
		})
}

type dataTransformMatViewScheme struct {
	matViewScheme
	transformDef string
	groupBy      string
}

func (cmvs *dataTransformMatViewScheme) BuildScheme(dbName string) (string, error) {
	return buildScheme(cmvs.dataStructure,
		cmvs.mvHead(dbName),
		func(data reflect.Type) string {
			return cmvs.transformDef
		},
		func(data reflect.Type) string {
			sql := fmt.Sprintf(" FROM %s.%s", dbName, cmvs.from)
			if cmvs.groupBy != "" {
				sql += fmt.Sprintf(" GROUP BY %s", cmvs.groupBy)
			}
			return sql
		})
}

func tableHead(name string) string {
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ", name)
}

func fullFieldsDefinition(data reflect.Type, skipPartitioningField bool, engine EngineType) string {
	if data.Kind() != reflect.Struct {
		return ""
	}
	fieldsDef := make([]string, 0, data.NumField())
	for i := 0; i < data.NumField(); i++ {
		field := data.Field(i)
		fieldDef := createFieldDefinition(field, engine)
		if fieldDef == "" {
			continue
		}
		if _, isPartField := field.Tag.Lookup("partitioning"); isPartField && skipPartitioningField {
			continue
		}
		if skip, skipOk := field.Tag.Lookup("skip"); skipOk && skip == "db" {
			continue
		}
		fieldsDef = append(fieldsDef, fieldDef)
	}
	return "(" + strings.Join(fieldsDef, ", ") + ")"
}

func createFieldDefinition(field reflect.StructField, engine EngineType) string {
	name, nameOk := field.Tag.Lookup("db")
	fieldType, typeOk := field.Tag.Lookup("type")
	if initialType, ok := field.Tag.Lookup("base_type"); ok && engine == Kafka {
		fieldType = initialType
	}
	if !nameOk || !typeOk {
		return ""
	}
	fieldDef := name + " " + fieldType
	defaultVal, defaultOk := field.Tag.Lookup("default")
	if defaultOk {
		if defaultVal == "" {
			defaultVal = "''"
		}
		fieldDef += " DEFAULT " + defaultVal
	}
	return fieldDef
}

// getDbFieldsPreInsertTransformation returns list of data transformation expressions, that should be applied to
// input values before inserting it to the logs table.
func getDbFieldsPreInsertTransformation(t reflect.Type) (exprs []string, err error) {
	defer func() {
		if r := recover(); r != nil {
			exprs = nil
			err = fmt.Errorf("panic in getDbFieldsPreInsertTransformation: %v", r)
		}
	}()

	exprs = make([]string, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.Tag.Get("skip") == "db" {
			continue
		}
		colName, dbColFound := field.Tag.Lookup("db")
		if !dbColFound {
			continue
		}
		if expr, found := field.Tag.Lookup("mv_transform"); found {
			exprs = append(exprs, fmt.Sprintf("%s as %s", expr, colName))
		} else {
			exprs = append(exprs, colName)
		}
	}

	return exprs, nil
}

func buildScheme(dataStruct reflect.Type, head string, fieldsList func(reflect.Type) string, engine func(reflect.Type) string) (res string, err error) {
	defer func() {
		if r := recover(); r != nil {
			res = ""
			err = fmt.Errorf("cannot create data scheme for %+v: %v", dataStruct, r)
		}
	}()

	if dataStruct == nil || dataStruct.NumField() == 0 {
		return "", errors.New("buildScheme: data structure is not set")
	}

	scheme := strings.Builder{}
	scheme.Grow(dataStruct.NumField() * 50)
	scheme.WriteString(head)
	scheme.WriteString(fieldsList(dataStruct))
	scheme.WriteString(engine(dataStruct))

	return scheme.String(), nil
}
