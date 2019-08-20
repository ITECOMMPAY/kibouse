package aggregations

import (
	"fmt"
	"strings"

	"kibouse/clickhouse"
	"kibouse/adapter/requests/queries"
	"kibouse/db"
)

// Aggregation is a common interface for data aggregation implementations.
type Aggregation interface {
	aggType() string
	SetAggName(string)
	AddCommonFilter(queries.Clause)
	SetSubAgg(Aggregation) error
	Aggregate(db.DataProvider) (*BucketAggregationData, error)
}

// Bucket represents common interface for accessing aggregation bucket data.
type Bucket interface {
	SetDocCount(uint64)
	DocCount() uint64
	Key() interface{}
	String() string
}

// MetricAggregationData contains aggregated metric data.
type MetricAggregationData struct {
	AggName string
	Value float64
}

// BucketAggregationData contains aggregated bucketing data.
type BucketAggregationData struct {
	AggName string
	Buckets bucketsStringer
}

type loader func(items interface{}) error

type aggFuncs []clickhouse.AggregationFunc

func (af *aggFuncs) appendAggregation(agg clickhouse.AggregationFunc) {
	if agg != nil && af != nil {
		*af = append(*af, agg)
	}
}

func (af *aggFuncs) appendMustCondToAggs(cond string) {
	for i := range *af {
		(*af)[i].AddMustCond(cond)
	}
}

func (af *aggFuncs) appendOptCondToAggs(cond string) {
	for i := range *af {
		(*af)[i].AddOptCond(cond)
	}
}

func (af aggFuncs) build() string {
	if len(af) == 0 {
		return ""
	}
	aggsString := make([]string, len(af))
	for i := range af {
		aggsString[i] = af[i].String()
	}
	return fmt.Sprintf("[ %s ] as results", strings.Join(aggsString, ","))
}

func createBaseAggregation() baseAggregation {
	return baseAggregation{
		name:         "",
		commonFilter: nil,
	}
}

type baseAggregation struct {
	name         string
	commonFilter queries.Clause
}

func (b *baseAggregation) SetAggName(name string) {
	b.name = name
}

func (b *baseAggregation) GetAggName() string {
	return b.name
}

func (b *baseAggregation) AddCommonFilter(filter queries.Clause) {
	if filter == nil {
		return
	}

	b.commonFilter = filter
}
