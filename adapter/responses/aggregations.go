package responses

type aggrBuckets struct {
	ResponseInputs
}

func (ab *aggrBuckets) CreateElasticJSON() (string, error) {
	return `{"aggregations":{` + ab.aggregation.String() + `}}`, nil
}

func NewAggOnlyResponseBuilder() Builder {
	return &aggrBuckets{}
}
