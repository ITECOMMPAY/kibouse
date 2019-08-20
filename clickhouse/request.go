package clickhouse

import (
	"fmt"
	"strings"

	"kibouse/db"
)


// NewRequestTpl creates new template of SQL request for data selection and aggregation.
func NewRequestTpl(index string) *db.Request {
	// modify elastic index name with wildcard symbol '*' according to regular expressions syntax and use it in Merge engine
	// for reading from all tables matched the pattern
	return db.NewRequest(
		fmt.Sprintf("merge(%s, '^%s')", db.DataBaseName, strings.Replace(index, "*", ".*", 1)),
		"*, _table",
	)
}
