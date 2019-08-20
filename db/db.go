package db

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"

	"kibouse/data/models"
	"kibouse/data/wrappers"
)

const (
	DataBaseName            = "logs"
	DataStorageTablesPrefix = "logs_"
)

// Scheme declares interface for various db creating objects.
type Scheme interface {
	BuildScheme(string) (string, error)
}

// DataProvider selects log records with specified structure from db by SQL requests.
type DataProvider interface {
	DataTable() string
	DataScheme() *models.ModelInfo
	FetchData(req *Request) (wrappers.ChDataWrapper, error)
	CreateDataSelector(req *Request) func(items interface{}) error
}

var logs *logsDB
var mutex = &sync.RWMutex{}

var notInitializedErr = errors.New("kibouse db connection is not initialized")

// InitLogsDbConnection creates new instance of logs database manager.
func InitLogsDbConnection(conn *sqlx.DB, dbName string) error {
	l, err := newLogsDB(conn, dbName)
	if err != nil {
		return err
	}
	mutex.Lock()
	logs = l
	mutex.Unlock()

	return nil
}

// CloseConnection closes connections pool and sets it to nil
func CloseLogsDbConnection() {
	if logs == nil {
		return
	}
	mutex.Lock()
	logs.conn.db.Close()
	logs = nil
	mutex.Unlock()
}

// GetTableByIndexPattern uses for searching db log tables by elasticsearch index pattern.
func GetTableByIndexPattern(pattern string) (string, error) {
	pattern = strings.TrimPrefix(pattern, ".")
	// index pattern contains wildcard symbol
	if strings.HasSuffix(pattern, "*") {
		return GetTableByPattern(pattern)
	}
	return pattern, nil
}

// CreateTable adds new table to database.
func CreateTable(scheme Scheme) error {
	if logs == nil {
		return notInitializedErr
	}
	if scheme == nil {
		return errors.New("empty data scheme")
	}
	schemeStr, err := scheme.BuildScheme(logs.dbName)

	if err != nil {
		return err
	}
	if _, err := logs.conn.db.Exec(schemeStr); err != nil {
		return errors.Wrap(err, "creating table error")
	}

	return logs.updateLogsTablesList()
}

// TableExists checks that table exists in the logs database.
func TableExists(table string) (bool, error) {
	if logs == nil {
		return false, notInitializedErr
	}

	mutex.RLock()
	defer mutex.RUnlock()

	return logs.tableExists(table)
}

// GetTableByPattern returns data tables matching the pattern.
func GetTableByPattern(pattern string) (string, error) {
	if logs == nil {
		return "", notInitializedErr
	}

	mutex.RLock()
	defer mutex.RUnlock()

	if table, ok := logs.findTable(pattern); !ok {
		return "", errors.New("no tables found by pattern " + pattern)
	} else {
		return table, nil
	}
}

// InsertIntoTable adds new entry to the table from kibouse db.
func InsertIntoTable(table string, insertion interface{}) error {
	if logs == nil {
		return notInitializedErr
	}
	return logs.insertIntoTable(table, insertion)
}

// Execute performs SQL query execution.
func Execute(query string) (sql.Result, error) {
	if logs == nil {
		return nil, notInitializedErr
	}
	if query == "" {
		return nil, errors.New("empty query")
	}

	mutex.RLock()
	defer mutex.RUnlock()

	return logs.conn.exec(query)
}

// CreateDataSelector returns function for loading data from db to the specific container.
func CreateDataSelector(req *Request) func(items interface{}) error {
	if logs == nil {
		return nil
	}
	if req == nil {
		return nil
	}

	mutex.RLock()
	defer mutex.RUnlock()

	return logs.conn.createDataLoader(req.Build())
}

// logsDB manages logs database structure.
type logsDB struct {
	conn   connection
	tables []string
	dbName string
}

func newLogsDB(db *sqlx.DB, dbName string) (*logsDB, error) {
	if db == nil {
		return nil, errors.New("nil db connection")
	}
	if dbName == "" {
		return nil, errors.New("database name is empty ")
	}

	logs := &logsDB{
		conn: connection{
			db: db,
		},
		dbName: dbName,
		tables: make([]string, 0),
	}

	return logs, logs.updateLogsTablesList()
}

func (l *logsDB) insertIntoTable(table string, insertion interface{}) error {
	req, err := buildInsertionExpr(l.dbName+"."+table, insertion)
	if err != nil {
		return err
	}
	_, err = l.conn.preparedExec(req, insertion)
	if err != nil {
		return errors.Wrap(err, "SQL failed to execute while inserting data")
	}

	return nil
}

func buildInsertionExpr(table string, dataStructure interface{}) (string, error) {
	fields, err := models.GetStructureTags(reflect.TypeOf(dataStructure), "db", false)
	if err != nil {
		return "", err
	}
	if table == "" || len(fields) == 0 {
		return "", errors.New("not enough data")
	}
	dbFieldsList := strings.Join(fields, ", ")
	for i := range fields {
		fields[i] = ":" + fields[i]
	}
	structFieldsLists := strings.Join(fields, ", ")
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, dbFieldsList, structFieldsLists), nil
}

func (l *logsDB) tableExists(table string) (bool, error) {
	return l.conn.exists(l.dbName + "." + table)
}

func (l *logsDB) updateLogsTablesList() error {
	l.tables = make([]string, 0)

	tablesExisted, err := l.conn.selectSingleColumn("Show tables from " + l.dbName)
	if err != nil {
		return errors.Wrap(err, "cannot read list of registered tables from database")
	}

	for i := range tablesExisted {
		if table := tablesExisted[i].(string); strings.HasPrefix(table, DataStorageTablesPrefix) {
			l.tables = append(l.tables, table)
		}
	}

	return nil
}

func (l *logsDB) findTable(pattern string) (string, bool) {
	pattern = strings.TrimSuffix(pattern, "*")

	for i := range l.tables {
		if strings.HasPrefix(l.tables[i], pattern) {
			return l.tables[i], true
		}
	}
	return "", false
}
