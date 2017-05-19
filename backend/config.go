package backend

import (
	"encoding/json"
	"io/ioutil"
)

type DBConf struct {
	User     string `json:"user"`
	Passwd   string `json:"passwd"`
	DBHost   string `json:"db_host"`
	DB       string `json:"db"`
	SyncHost string `json:"sync_host"`
	TableGap int    `json:"table_gap"`
	DBGap    int    `json:"db_gap"`
	DBName   string `json:"db_name"`
}

func LoadConfig(path string) (*DBConf, error) {
	def := DBConf{
		User:   "test",
		Passwd: "test",
		DBHost: "127.0.0.1:8080",
	}

	cfgbyte, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(cfgbyte, &def)
	if err != nil {
		return nil, err
	}

	return &def, nil
}
