package handlers

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/pkg/errors"

	"kibouse/clickhouse"
	"kibouse/data/wrappers"
	"kibouse/data/models"
	"kibouse/db"
	"kibouse/adapter/responses"
	"kibouse/adapter/settings"
	"kibouse/adapter/requests"
	"kibouse/adapter/requests/queries"
)

// UpdateSettingsHandler updates and inserts kibana settings entry by its id
func UpdateSettingsHandler(context HandlerContext) http.HandlerFunc {
	handler := func(w http.ResponseWriter, r *http.Request) {
		t, ok := context.URL.FetchParam(r, "type")
		if !ok {
			writeResponseError(w, errors.New("cannot fetch element type from url"), http.StatusBadRequest, context.RuntimeLog)
			return
		}
		id, ok := context.URL.FetchParam(r, "id")
		if !ok {
			id = fmt.Sprintf("%s_%d", t, time.Now().UnixNano())
		}

		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			writeResponseError(w, errors.New("error in getting uri body"), http.StatusBadRequest, context.RuntimeLog)
			return
		}

		var elasticSettings settings.ElasticSettings
		err = json.Unmarshal(body, &elasticSettings)

		// for updating kibana table entries, data may be wrapped by additional service structure {"doc":{...}}
		if strings.Contains(r.RequestURI, "/_update") {
			doc := struct {
				Doc settings.ElasticSettings `json:"doc"`
			}{}
			err = json.Unmarshal(body, &doc)
			elasticSettings = doc.Doc
		}
		if err != nil {
			writeResponseError(w, err, http.StatusBadRequest, context.RuntimeLog)
			return
		}

		elasticSettings.Type = t
		elasticSettingsItem := settings.CreateElasticSettingsItem(id, &elasticSettings)

		provider, err := clickhouse.NewProvider(models.SettingsTableName)
		if err != nil {
			writeResponseError(w, err, http.StatusInternalServerError, context.RuntimeLog)
			return
		}

		err = update(settings.ElasticToClickhouse(elasticSettingsItem), provider)
		if err != nil {
			writeResponseError(w, err, http.StatusInternalServerError, context.RuntimeLog)
			return
		}

		response := responses.CreateUpdatingResponse(t, id)
		writeResponseSuccess(w, &response)
	}
	if context.HttpLog != nil {
		return logHttpTransactions(context.HttpLog, handler)
	}
	return handler
}

func DeleteSettingsHandler(context HandlerContext) http.HandlerFunc {
	handler := func(w http.ResponseWriter, r *http.Request) {
		body, err := readBody(w, r, context.RuntimeLog)
		if err != nil {
			return
		}
		provider, err := clickhouse.NewProvider(models.SettingsTableName)
		if err != nil {
			writeResponseError(w, err, http.StatusInternalServerError, context.RuntimeLog)
			return
		}

		esReq, err := requests.ParseElasticJSON(body, provider.DataScheme())
		if err != nil {
			writeResponseError(w, errors.Wrap(err,"cannot parse elastic request body"), http.StatusBadRequest, context.RuntimeLog)
			return
		}

		err = removeByCond(esReq.Query, provider)
		if err != nil {
			writeResponseError(w, err, http.StatusBadRequest, context.RuntimeLog)
			return
		}

	}
	if context.HttpLog != nil {
		return logHttpTransactions(context.HttpLog, handler)
	}
	return handler
}

// update updates kibana settings table with new config entry.
func update(cfg *models.ClickhouseSettings, conn db.DataProvider) error {
	err := removeByID(cfg.ID, conn)
	if err != nil {
		return err
	}

	return insert(cfg)
}

// removeByID updates settings table row as marked for collapsing.
func removeByID(id string, conn db.DataProvider) error {
	return removeByCond(queries.NewStringMatch("_id", id), conn)
}

func findSettings(query queries.Clause, conn db.DataProvider) (*wrappers.KibanaSettings, error) {
	rows, err := conn.FetchData(clickhouse.NewRequestTpl(models.SettingsTableName).Where(query.String()).Final(true))
	if err != nil {
		return nil, errors.Wrap(err, "kibana settings selection failed")
	}
	cfg, ok := rows.(*wrappers.KibanaSettings)
	if !ok {
		return nil, errors.New("converting kibana configuration items to KibanaSettings failed")
	}

	return cfg, nil
}

func removeByCond(query queries.Clause, conn db.DataProvider) error {
	cfg, err := findSettings(query, conn)
	if err != nil {
		return err
	}

	for item := cfg.NextClickhouseSettings(); item != nil; item = cfg.NextClickhouseSettings() {
		item.Sign = -1
		err := insert(item)
		if err != nil {
			return errors.Wrap(err, "cannot remove kibana settings record with id = " + item.ID)
		}
	}

	return nil
}

func insert(cfg *models.ClickhouseSettings) error {
	return db.InsertIntoTable(models.SettingsTableName, *cfg)
}
