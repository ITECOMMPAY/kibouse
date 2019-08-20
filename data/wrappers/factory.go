package wrappers

import (
	"github.com/pkg/errors"
)

var factories = map[string]func() (ChDataWrapper, error){}

// CreateDataContainer creates container for logs by its db table name
func CreateDataContainer(table string) (ChDataWrapper, error) {
	if factory, ok := factories[table]; ok {
		return factory()
	}

	return nil, errors.New("cannot create data container for unknown table: " + table)
}
