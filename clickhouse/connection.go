package clickhouse

import (
	"github.com/jmoiron/sqlx"
	_ "github.com/kshvakov/clickhouse"
	"github.com/pkg/errors"
)

// CreateConnection returns new sqlx.DB connection to clickhouse and function for generating information about
// fields type and internal name
func CreateConnection(source string) (*sqlx.DB, error) {
	connect, err := sqlx.Open("clickhouse", source)
	if err != nil {
		return nil, errors.New("cannot establish connection to the clickhouse - " + err.Error())
	}
	if err = connect.Ping(); err != nil {
		return nil, errors.New("clickhouse server is not responding - " + err.Error())
	}

	return connect, nil
}
