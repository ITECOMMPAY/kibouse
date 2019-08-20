package clickhouse

import "fmt"

// AggregationFunc interface for creating clickhouse aggregation functions.
type AggregationFunc interface {
	AddMustCond(string)
	AddOptCond(string)
	String() string
	Copy() interface{}
}

type countAggregation struct {
	cond string
}

func (ca *countAggregation) String() string {
	if ca.cond != "" {
		return fmt.Sprintf("countIf(%s)", ca.cond)
	}

	return "count()"
}

func (ca *countAggregation) AddMustCond(cond string) {
	if ca.cond == "" {
		ca.cond = cond
	} else {
		ca.cond = fmt.Sprintf("(%s) AND (%s)", ca.cond, cond)
	}
}

func (ca *countAggregation) AddOptCond(cond string) {
	if ca.cond == "" {
		ca.cond = cond
	} else {
		ca.cond = fmt.Sprintf("(%s) OR (%s)", ca.cond, cond)
	}
}

func (ca *countAggregation) Copy() interface{} {
	return &countAggregation{
		cond: ca.cond,
	}
}

// NewCountAggregation creates new count or countIf aggregation.
func NewCountAggregation(condition string) *countAggregation {
	if condition == "" {
		return &countAggregation{}
	}
	return &countAggregation{cond: condition}
}

type sumAggregation struct {
	column string
	cond   string
}

func (sa *sumAggregation) String() string {
	if sa.cond != "" {
		return fmt.Sprintf("sumIf(%s, %s)", sa.column, sa.cond)
	}

	return fmt.Sprintf("sum(%s)", sa.column)
}

func (sa *sumAggregation) AddMustCond(cond string) {
	if sa.cond == "" {
		sa.cond = cond
	} else {
		sa.cond = fmt.Sprintf("(%s) AND (%s)", sa.cond, cond)
	}
}

func (sa *sumAggregation) AddOptCond(cond string) {
	if sa.cond == "" {
		sa.cond = cond
	} else {
		sa.cond = fmt.Sprintf("(%s) OR (%s)", sa.cond, cond)
	}
}

func (sa *sumAggregation) Copy() interface{} {
	return &sumAggregation{
		cond:   sa.cond,
		column: sa.column,
	}
}

// NewSumAggregation creates new sum or sumIf aggregation.
func NewSumAggregation(column string, condition string) *sumAggregation {
	if column == "" {
		return nil
	}
	agg := &sumAggregation{column: column, cond: condition}
	return agg
}
