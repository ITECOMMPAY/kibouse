package aggregations

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	"kibouse/clickhouse"
	"kibouse/data/models"
	"kibouse/db"
	"kibouse/adapter/requests/queries"
)

const (
	unsupported = 0
	day         = time.Hour * 24
	week        = day * 7

	preparedDataPeriod = int64(time.Minute * 5)

	DataHistogramAggType = "DateHistogram"
)

// CreateDateHistogramAgg returns new histogram aggregation struct
func CreateDateHistogramAgg(
	interval string,
	fieldName string,
	dataRange *queries.RangeClause,
	optimization bool,
) (*DateHistogram, error) {
	intervalSettings, err := parseHistogramInterval(interval)
	if err != nil {
		return nil, err
	}
	if dataRange == nil || dataRange.GetField() != fieldName {
		return nil, errors.New("data range for histogram is not set")
	}

	histogram := &DateHistogram{
		baseAggregation:  createBaseAggregation(),
		fieldName:        fieldName,
		interval:         intervalSettings.calcInterval(),
		timeOptimization: optimization,
	}

	return histogram, nil
}

type DateHistogram struct {
	baseAggregation
	filters          *Filters
	interval         int64
	fieldName        string
	timeOptimization bool
}

func (hs *DateHistogram) optimizationRequired() bool {
	return hs.timeOptimization && hs.interval >= preparedDataPeriod
}

func (hs *DateHistogram) aggType() string {
	return DataHistogramAggType
}

func (hs *DateHistogram) SetSubAgg(agg Aggregation) error {
	if agg == nil {
		return nil
	}
	switch a := agg.(type) {
	case *Filters:
		hs.filters = a
		return nil
	default:
		return errors.New("unsupported sub aggregation type: " + agg.aggType())
	}
}

func (hs *DateHistogram) Aggregate(conn db.DataProvider) (*BucketAggregationData, error) {
	index := conn.DataTable()
	if index == "" {
		return nil, errors.New("index pattern is not set for data provider")
	}

	histogram := &BucketAggregationData {
		AggName: hs.name,
		Buckets: bucketsStringer{
			Buckets: make([]Bucket, 0),
			stringer: bucketsToArrayJSON,
		},
	}

	request := hs.createDataAggregatingRequest(index)
	if request == nil {
		return histogram, nil
	}

	query := request.Build()
	print("\n aggregation req: ", query)

	histogramBuckets, err := calcHistogram(conn.CreateDataSelector(request))
	if err != nil {
		return nil, err
	}

	for {
		rawBucketData, ok := histogramBuckets.fetch()
		if !ok {
			break
		}

		column := column{
			bucket: bucket {
				key: time.Unix(0, hs.interval*rawBucketData.Key).In(time.Local),
			},
		}

		counts := rawBucketData.Vals

		if column.docCount = counts[len(counts)-1]; column.docCount > 0 {
			var subAggBuckets BucketAggregationData
			if hs.filters != nil {
				subAggBuckets = hs.filters.createBuckets()
			}

			if subAggBuckets.Buckets.Buckets != nil {
				for j := range subAggBuckets.Buckets.Buckets {
					subAggBuckets.Buckets.Buckets[j].SetDocCount(counts[j])
				}
			}

			column.subAggData = subAggBuckets
			histogram.Buckets.Buckets = append(histogram.Buckets.Buckets, &column)
		}
	}

	return histogram, nil
}

func (hs *DateHistogram) createAggFuncs() aggFuncs {
	if hs.filters != nil {
		return append(hs.filters.createAggFuncs(), clickhouse.NewCountAggregation(""))
	}
	return aggFuncs{clickhouse.NewCountAggregation("")}
}

func (hs *DateHistogram) createDataAggregatingRequest(index string) *db.Request {
	filterConditions := queries.GetSimpleClausesList(hs.commonFilter)

	// histogram calc optimization performs only for log entries count visualization (discover) without any additional filters.
	var request *db.Request
	if len(filterConditions) == 1 && hs.optimizationRequired() {
		request = clickhouse.NewRequestTpl(models.PreparedHistogramDataTablePrefix + index)
		request.What(
			fmt.Sprintf(
				"%s, toInt64(key / %d) as cur_key",
				aggFuncs{clickhouse.NewSumAggregation("count", "")}.build(),
				hs.interval/preparedDataPeriod,
			),
		)
		timeRange := queries.NewRange(fmt.Sprintf("(key * %d)", preparedDataPeriod), false)
		if origRange, ok := filterConditions[0].(*queries.RangeClause); ok {
			// exclude upper bound value from interval, because key from prepared data contains interval lower bounds
			upperBound, _ := origRange.GetUpper()
			timeRange.AddUpper(upperBound, true)
			timeRange.AddLower(origRange.GetLower())
		}
		hs.commonFilter = timeRange
		print("\n Aggregation optimized ")
	} else {
		request = clickhouse.NewRequestTpl(index)
		request.What(hs.createAggFuncs().build())
		request.AppendToWhat(fmt.Sprintf("toInt64((%s) / %d) as cur_key", hs.fieldName, hs.interval))
	}

	request.GroupBy("cur_key")
	request.OrderBy(queries.NewSortSection(map[string]queries.Order{"cur_key": queries.Asc}).String())

	request.WhereAnd(hs.commonFilter.String())

	return request
}

type histogramInterval struct {
	timeUnit time.Duration
	timeVal  int64
}

func (i *histogramInterval) calcInterval() int64 {
	return i.timeUnit.Nanoseconds() * i.timeVal
}

func parseHistogramInterval(interval string) (histogramInterval, error) {
	histogramInterval := histogramInterval{}
	for _, item := range []struct {
		name     string
		duration time.Duration
	}{
		{"ms", time.Millisecond},
		{"s", time.Second},
		{"m", time.Minute},
		{"h", time.Hour},
		{"d", day},
		{"w", week},
	} {
		if strings.HasSuffix(interval, item.name) {
			interval = strings.TrimSuffix(interval, item.name)
			histogramInterval.timeUnit = item.duration
			break
		}
	}
	if histogramInterval.timeUnit == unsupported {
		return histogramInterval, errors.New("unsupported histogram period units")
	}

	timeVal, err := strconv.Atoi(interval)
	if err != nil {
		return histogramInterval, err
	}
	histogramInterval.timeVal = int64(timeVal)
	return histogramInterval, nil
}

type histogramCounts struct {
	Vals []uint64 `db:"results"`
	Key  int64    `db:"cur_key"`
}


func calcHistogram(loader loader) (*HistogramRawBuckets, error) {
	results := HistogramRawBuckets{}
	if err := results.fill(loader); err != nil {
		return nil, err
	}
	return &results, nil
}

type HistogramRawBuckets struct {
	BucketsData []histogramCounts
	index       int
}

func (mac *HistogramRawBuckets) fill(loader loader) error {
	mac.BucketsData = make([]histogramCounts, 0)
	mac.index = 0
	err := loader(&mac.BucketsData)
	return err
}

func (mac *HistogramRawBuckets) fetch() (histogramCounts, bool) {
	if mac.index < len(mac.BucketsData) {
		mac.index++
		return mac.BucketsData[mac.index-1], true
	}
	return histogramCounts{}, false
}

func (mac *HistogramRawBuckets) reset() {
	mac.index = 0
}

type column struct {
	bucket
}

func (c column) String() string {
	intervalTime := c.key.(time.Time)
	// kibana requires integer representation of time in milliseconds
	milliseconds := intervalTime.UnixNano() / int64(time.Millisecond)

	return fmt.Sprintf(
		`{%s, "key_as_string":"%s","key":%d}`,
		c.bucket.String(),
		intervalTime.Format(time.UnixDate),
		milliseconds,
	)
}