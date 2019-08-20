package db

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

type connection struct {
	db *sqlx.DB
}

func (c *connection) createDataLoader(query string) func(items interface{}) error {
	return func(items interface{}) error {
		return c.db.Select(items, query)
	}
}

func (c *connection) selectSingleColumn(query string) ([]interface{}, error) {
	result := make([]interface{}, 0)
	rows, err := c.db.Query(query)
	if err != nil {
		return nil, errors.Wrap(err, "SQL data querying failed: "+query)
	}
	defer rows.Close()
	for rows.Next() {
		var val interface{}
		if err := rows.Scan(&val); err != nil {
			return nil, errors.Wrap(err, "Data rows scanning error")
		}
		result = append(result, val)
	}
	return result, nil
}

func (c *connection) exec(query string) (sql.Result, error) {
	res, err := c.db.Exec(query)
	if err != nil {
		err = errors.Wrap(err, "SQL query execution failed: "+query)
	}
	return res, err
}

func (c *connection) preparedExec(query string, inserted interface{}) (sql.Result, error) {
	trans := c.db.MustBegin()
	result, err := trans.NamedExec(query, inserted)
	if err != nil {
		err = errors.Wrap(err, "SQL query prepared execution failed: "+query)
	}
	trans.Commit()
	return result, err
}

func (c *connection) exists(table string) (bool, error) {
	var result uint8
	err := c.db.Get(&result, "EXISTS TABLE " + table)
	return result == 1, err
}