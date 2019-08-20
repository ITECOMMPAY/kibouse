package setup

import (
	"reflect"

	"kibouse/clickhouse"
	"kibouse/config"
	"kibouse/data/models"
	"kibouse/db"
)

// InitialSetup prepares environment before first application run:
// recreate internal data tables, init kibana configuration
func InitialSetup(cfg *config.AppConfig) error {
	if err := initDBStruct(cfg); err != nil {
		return err
	}

	return initKibanaSettings(cfg.KibanaVersion())
}

// CreateKibanaSettingsTable creates clickhouse table with kibana settings.
func CreateKibanaSettingsTable() error {
	scheme := clickhouse.CreateCollapsingMergeTreeTableScheme(
		models.SettingsTableName,
		reflect.TypeOf(models.ClickhouseSettings{}),
		"sign",
		clickhouse.DefaultIndexGranularity,
	)
	return db.CreateTable(scheme)
}

func initKibanaSettings(version string) error {
	clickhouseCfg := models.NewClickhouseSettings(version, "config")

	if err := db.InsertIntoTable(models.SettingsTableName, *clickhouseCfg); err != nil {
		return err
	}

	return nil
}

func initDBStruct(cfg *config.AppConfig) error {
	if _, err := db.Execute("CREATE DATABASE IF NOT EXISTS " + db.DataBaseName); err != nil {
		return err
	}

	if cfg.ReinitRequired() {
		if _, err := db.Execute("DROP TABLE IF EXISTS " + db.DataBaseName + "." + models.SettingsTableName); err != nil {
			return err
		}
	}

	if err := CreateKibanaSettingsTable(); err != nil {
		return err
	}

	return nil
}
