package aggregations

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"kibouse/clickhouse"
	"kibouse/adapter/requests/queries"
	"kibouse/db"
)

const FiltersAggType = "Filters"

// FilterSettings contains name and filter condition
// for aggregation 'Filters'
type FilterSettings struct {
	Condition queries.Clause
	Name      string
}

type Filters struct {
	baseAggregation
	filters []FilterSettings
}

// CreateFiltersAgg returns new Filters aggregation structure
func CreateFiltersAgg(settings []FilterSettings) *Filters {
	return &Filters{baseAggregation: createBaseAggregation(), filters: settings}
}

func (f *Filters) aggType() string {
	return FiltersAggType
}

func (f *Filters) createAggFuncs() aggFuncs {
	funcs := make(aggFuncs, len(f.filters))

	for i := 0; i < len(f.filters); i++ {
		cond := ""
		if f.filters[i].Condition != nil {
			cond = f.filters[i].Condition.String()
		}
		funcs[i] = clickhouse.NewCountAggregation(cond)
	}

	return funcs
}

func (f *Filters) createBuckets() BucketAggregationData {
	buckets := make([]Bucket, len(f.filters))
	for i := range f.filters {
		buckets[i] = &filterBucket{
			bucket: bucket{
				key: f.filters[i].Name,
			},
		}
	}

	return BucketAggregationData{
		AggName: f.name,
		Buckets: bucketsStringer{
			Buckets: buckets,
			stringer: bucketsToObjJSON,
		},
	}
}

func (f *Filters) SetSubAgg(agg Aggregation) error {
	if agg == nil {
		return nil
	}
	return errors.New("unsupported sub aggregation type: " + agg.aggType())
}

func (f *Filters) Aggregate(conn db.DataProvider) (*BucketAggregationData, error) {
	return nil, nil
}

type filterBucket struct {
	bucket
}

func (fb filterBucket) String() string {
	return fmt.Sprintf(
		`"%s":{%s}`,
		strings.Replace(fb.key.(string), `"`, `\"`, -1),
		fb.bucket.String(),
	)
}