package app

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
	"log"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"

	"kibouse/app/handlers"
	"kibouse/clickhouse"
	"kibouse/config"
	"kibouse/data/models"
	"kibouse/db"
	"kibouse/logging"
	"kibouse/setup"
)

type proxyTarget int

const (
	proxyToElastic proxyTarget = iota
	adapterToClickhouse
)

type handlerRoutes struct {
	route   string
	handler handlers.HandlerConstructor
}

// App is the main type holding app's settings and processing external http requests
type App struct {
	sync.RWMutex
	cfg    *config.AppConfig
	server *http.Server
	logger *logrus.Logger
}

// NewReverseProxy reads the application config and uses it for creating new instance of App
func New(cfgPath string) (*App, error) {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return nil, err
	}

	app := &App{
		cfg:    cfg,
		logger: createLogger(cfg),
	}

	if err := app.init(); err != nil {
		return nil, err
	}

	return app, nil
}

// Run starts listening external requests by app
func (app *App) Run() error {
	defer db.CloseLogsDbConnection()
	return app.server.ListenAndServe()
}

func (app *App) init() error {
	app.Lock()
	defer app.Unlock()

	connection, err := clickhouse.CreateConnection(app.cfg.GetClickhouseSource())
	if err != nil {
		return err
	}

	if err = db.InitLogsDbConnection(connection, db.DataBaseName); err != nil {
		return err
	}

	// reinit kibouse db
	if app.cfg.ReinitRequired() {
		if err := setup.InitialSetup(app.cfg); err != nil {
			return err
		}
	}

	// create tables for logs delivery and storing (if not exists yet)
	if app.cfg.CreateChTables() {
		tables := models.GetLogsTablesSchemas()
		for tableName := range tables {
			ok, err := db.TableExists(tableName)
			if err != nil {
				return err
			}
			if ok {
				continue
			}
			err = clickhouse.CreateDataDeliveryQueue(tableName, tableName, tables[tableName], app.cfg.GetKafkaSource())
			if err != nil {
				logrus.Error(fmt.Sprintf("%+v", err))
			}
		}
	}

	r := mux.NewRouter()

	var targeting = adapterToClickhouse
	if app.cfg.ProxyToElastic() {
		targeting = proxyToElastic
	}

	var httpLogger *logging.HttpLog
	if app.cfg.LogRequests() {
		f, err := os.OpenFile(app.cfg.HttpTransLogFile(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			logrus.Error(fmt.Sprintf("error openning http transactions log %s", err.Error()))
		}
		httpLogger = logging.NewHttpLogger(log.New(f, "", log.Ltime|log.Lshortfile))
	}

	context := handlers.HandlerContext{
		URL:        &urlParser{},
		RuntimeLog: app.logger,
		HttpLog:    httpLogger,
		Cfg:        app.cfg,
	}

	// create handlers for all API endpoints
	for _, handler := range initAppHandlerParams(targeting) {
		r.PathPrefix(handler.route).HandlerFunc(handler.handler(context))
	}

	app.server = createServer(app.cfg.GetListeningPort(), r)

	return nil
}

func initAppHandlerParams(target proxyTarget) []handlerRoutes {
	if target == adapterToClickhouse {
		return []handlerRoutes{
			{
				route:   "/_msearch",
				handler: handlers.MultiSearchHandler,
			},
			{
				route:   "/{index}/_msearch",
				handler: handlers.MultiSearchHandler,
			},
			{
				route:   "/.kibana/{type:visualization|dashboard|config|search|url|server|timelion-sheet|index-pattern}/{id}",
				handler: handlers.UpdateSettingsHandler,
			},
			{
				route:   "/.kibana/{type:visualization|dashboard|config|search|url|server|timelion-sheet|index-pattern}",
				handler: handlers.UpdateSettingsHandler,
			},
			{
				route:   "/.kibana/_delete_by_query",
				handler: handlers.DeleteSettingsHandler,
			},
			{
				route:   "/_mget",
				handler: handlers.MultiGetRequestsHandler,
			},
			{
				route:   "/{index}/_search",
				handler: handlers.SearchHandler,
			},
			{
				route:   "/{index}/_field_caps",
				handler: handlers.ElasticMappingBuildHandler,
			},
			{
				route:   "/",
				handler: handlers.StaticRequestsHandler,
			},
		}
	}
	if target == proxyToElastic {
		return []handlerRoutes{
			{
				route:   "/",
				handler: handlers.LoggingElasticReverseProxy,
			},
		}
	}
	return nil
}

func createLogger(cfg *config.AppConfig) *logrus.Logger {
	logger := logrus.New()
	if cfg.LogDebugMessages() {
		logger.SetLevel(logrus.DebugLevel)
	} else {
		logger.SetLevel(logrus.WarnLevel)
	}
	logger.Out = os.Stderr

	return logger
}

func createServer(port string, r *mux.Router) *http.Server {
	return &http.Server{
		Addr:    ":" + port,
		Handler: r,
		// Disable HTTP/2.
		TLSNextProto: make(map[string]func(*http.Server, *tls.Conn, http.Handler)),
		WriteTimeout: 30 * time.Minute,
	}
}

type urlParser struct {
}

func (*urlParser) FetchParam(r *http.Request, name string) (value string, ok bool) {
	vars := mux.Vars(r)
	value, ok = vars[name]
	return
}
