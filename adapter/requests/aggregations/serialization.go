package aggregations

import (
	"fmt"
	"strings"
)

type bucket struct {
	subAggData fmt.Stringer
	key interface{}
	docCount uint64
}

func (b *bucket) SetDocCount(cnt uint64) {
	b.docCount = cnt
}

func (b bucket) DocCount() uint64 {
	return b.docCount
}

func (b bucket) Key() interface{} {
	return b.key
}

func (b bucket) buildSubAggStr() string {
	if b.subAggData != nil {
		return b.subAggData.String()
	}
	return ""
}

func (b bucket) String() string {
	subAgg := b.buildSubAggStr()
	if subAgg != "" {
		subAgg += ","
	}

	return fmt.Sprintf(`%s "doc_count":%d`, subAgg, b.docCount)
}

type bucketsStringer struct {
	Buckets []Bucket
	stringer func([]Bucket) string
}

func (bs bucketsStringer) String() string {
	if bs.Buckets == nil || bs.stringer == nil {
		return ""
	}
	return bs.stringer(bs.Buckets)
}

func bucketsToStrings(buckets []Bucket) []string {
	strBuckets := make([]string, len(buckets))

	for i, b := range buckets {
		strBuckets[i] = b.String()
	}

	return strBuckets
}

func bucketsToObjJSON(buckets []Bucket) string{
	strBuckets := bucketsToStrings(buckets)

	return fmt.Sprintf("{%s}", strings.Join(strBuckets, ","))
}

func bucketsToArrayJSON(buckets []Bucket) string {
	strBuckets := bucketsToStrings(buckets)

	return fmt.Sprintf("[%s]", strings.Join(strBuckets, ","))
}

func (mad MetricAggregationData) String() string {
	if mad.AggName == "" {
		return ""
	}
	return fmt.Sprintf(`"%s":{value:%f}`, mad.AggName, mad.Value)
}

func (mad MetricAggregationData) MarshalJSON() ([]byte, error) {
	return []byte("{" + mad.String() + "}"), nil
}

func (bad BucketAggregationData) DocCount() uint64 {
	var count uint64
	for _, b := range bad.Buckets.Buckets {
		count += b.DocCount()
	}
	return count
}

func (bad BucketAggregationData) String() string {
	if bad.AggName == "" {
		return ""
	}
	return fmt.Sprintf(`"%s":{"buckets":%s}`, bad.AggName, bad.Buckets)
}

func (bad BucketAggregationData) MarshalJSON() ([]byte, error) {
	return []byte("{" + bad.String() + "}"), nil
}
