package clickhouse

import (
	"github.com/pkg/errors"

	"kibouse/data/wrappers"
	"kibouse/data/models"
	"kibouse/db"
)

// ChDataProvider selects from Clickhouse log records with specified structure by SQL requests.
type ChDataProvider struct {
	data  wrappers.ChDataWrapper
	table string
}

// DataTable returns clickhouse table name with data provided,
func (dp *ChDataProvider) DataTable() string {
	if dp == nil {
		return ""
	}
	return dp.table
}

// DataScheme returns data model scheme definition.
func (dp *ChDataProvider) DataScheme() *models.ModelInfo {
	if dp != nil && dp.data != nil {
		return dp.data.ModelScheme()
	}
	return nil
}

// FetchData selects data from specified table using SQL requests.
func (dp *ChDataProvider) FetchData(req *db.Request) (wrappers.ChDataWrapper, error) {
	if dp == nil || dp.data == nil {
		return nil, errors.New("table pattern is not set")
	}

	if err := dp.data.FetchData(db.CreateDataSelector(req)); err != nil {
		return nil, errors.Wrap(err, "SQL failed to execute while querying data: "+req.Build())
	}

	return dp.data, nil
}

// CreateDataSelector returns function for selecting data from db and putting it into the structure.
func (dp *ChDataProvider) CreateDataSelector(req *db.Request) func(interface{}) error {
	return db.CreateDataSelector(req)
}

func newProvider(table string, data wrappers.ChDataWrapper) *ChDataProvider {
	return &ChDataProvider{
		data:  data,
		table: table,
	}
}

// NewProvider creates new object for fetching logs records from DB by SQL requests.
func NewProvider(index string) (db.DataProvider, error) {
	table, err := db.GetTableByIndexPattern(index)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create data provider for index " + index)
	}
	wrapper, err := wrappers.CreateDataContainer(table)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create data provider for table " + table)
	}

	return newProvider(table, wrapper), nil
}
