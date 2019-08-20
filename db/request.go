package db

import (
	"fmt"
	"strings"
)

const (
	requestTpl = "SELECT %s FROM %s"
	whereTpl   = "WHERE %s"
	groupByTpl = "GROUP BY %s"
	havingTbl  = "HAVING %s"
	orderByTpl = "ORDER BY %s"
	limitTpl   = "LIMIT %d"
)

type SortOrder string

const (
	ASC  SortOrder = "ASC"
	DESC SortOrder = "DESC"
)

// Request contains data for select request generation.
type Request struct {
	tables  string
	what    string
	where   string
	sorting string
	group   string
	having  string
	limit   string
	final   bool
	isEmpty bool
}

// NewRequest creates new database request to the table specified.
func NewRequest(from string, what ...string) *Request {
	request := Request{tables: from}
	if len(what) > 0 {
		request.what = what[0]
	}
	return &request
}

// From specifies the name of table from which data will be selected.
func (t *Request) From(table string) *Request {
	if table != "" {
		t.tables = table
	}
	return t
}

// Final adds flag Final to request.
func (t *Request) Final(fin bool) *Request {
	t.final = fin
	return t
}

// What specifies required data.
func (t *Request) What(requiredData string) *Request {
	if requiredData != "" {
		t.what = requiredData
	}
	return t
}

// What specifies required data.
func (t *Request) AppendToWhat(requiredData string) *Request {
	if t.what != "" && requiredData != "" {
		t.what += ", " + requiredData
	} else {
		t.What(requiredData)
	}
	return t
}

// Where specifies select request condition.
func (t *Request) Where(cond string) *Request {
	if cond != "" {
		if str := cond; str != "" {
			t.where = fmt.Sprintf(whereTpl, str)
		}
	}
	return t
}

// WhereAnd adds new mandatory condition to the request.
func (t *Request) WhereAnd(cond string) *Request {
	if cond != "" && t.where != "" {
		t.where += fmt.Sprintf(" AND (%s)", cond)
		return t
	}
	return t.Where(cond)
}

// WhereOr adds new optional condition to the request.
func (t *Request) WhereOr(cond string) *Request {
	if cond != "" && t.where != "" {
		t.where += fmt.Sprintf(" OR (%s)", cond)
		return t
	}
	return t.Where(cond)
}

// OrderBy sets output data sorting sorting.
func (t *Request) OrderBy(field string, order ...SortOrder) *Request {
	if field != "" {
		t.sorting = fmt.Sprintf(orderByTpl, field)
		if len(order) > 0 {
			t.sorting += " " + string(order[0])
		}
	}
	return t
}

// GroupBy adds data grouping by field.
func (t *Request) GroupBy(val string) *Request {
	if val != "" {
		t.group = fmt.Sprintf(groupByTpl, val)
	}
	return t
}

// Having adds post aggregation filter.
func (t *Request) Having(cond string) *Request {
	if t != nil && cond != "" {
		t.having = fmt.Sprintf(havingTbl, cond)
	}
	return t
}

// Limit sets maximum number of output data rows.
func (t *Request) Limit(limit int) *Request {
	t.isEmpty = limit == 0
	t.limit = fmt.Sprintf(limitTpl, limit)
	return t
}

func (t *Request) IsEmpty() bool {
	return t.isEmpty
}

// Build creates string representation of request.
func (t *Request) Build() string {
	req := strings.Builder{}
	req.WriteString(fmt.Sprintf(requestTpl, t.what, t.tables))
	if t.final {
		req.WriteString(" FINAL")
	}
	// append condition
	req.WriteString(" " + t.where)
	// append data grouping
	req.WriteString(" " + t.group)
	// append post aggregation filtering
	req.WriteString(" " + t.having)
	// append sorting sorting
	req.WriteString(" " + t.sorting)
	// append rows limit
	req.WriteString(" " + t.limit)

	return req.String()
}
