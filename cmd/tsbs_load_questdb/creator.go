package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type dbCreator struct {
	questdbRESTEndPoint string
}

func (d *dbCreator) Init() {
	d.questdbRESTEndPoint = questdbRESTEndPoint
}

func (d *dbCreator) DBExists(dbName string) bool {
	r, err := execQuery(questdbRESTEndPoint, "SHOW TABLES")
	if err != nil {
		panic(fmt.Errorf("fatal error, failed to query questdb: %s", err))
	}

	// 检查是否已存在相关表格，但不终止程序
	tableExists := false
	for i, v := range r.Dataset {
		if i >= 0 {
			tableName := v[0].(string)
			if tableName == "cpu" || tableName == "readings" || tableName == "diagnostics" {
				fmt.Printf("Table %s already exists\n", tableName)
				tableExists = true
			}
		}
	}

	//if !tableExists {
	//	// 创建必要的表格结构
	//	// 创建 CPU 表
	//	_, err = execQuery(questdbRESTEndPoint, "CREATE TABLE IF NOT EXISTS cpu (hostname SYMBOL, region SYMBOL, datacenter SYMBOL, rack SYMBOL, os SYMBOL, arch SYMBOL, team SYMBOL, service SYMBOL, service_version SYMBOL, service_environment SYMBOL, usage_user LONG, usage_system LONG, usage_idle LONG, usage_nice LONG, usage_iowait LONG, usage_irq LONG, usage_softirq LONG, usage_steal LONG, usage_guest LONG, usage_guest_nice LONG, timestamp TIMESTAMP) timestamp(timestamp) PARTITION BY DAY")
	//	if err != nil {
	//		panic(fmt.Errorf("fatal error, failed to create cpu table: %s", err))
	//	}
	//
	//	// 创建 IoT readings 表
	//	_, err = execQuery(questdbRESTEndPoint, "CREATE TABLE IF NOT EXISTS readings (name SYMBOL, driver SYMBOL, fleet SYMBOL, model SYMBOL, load_capacity DOUBLE, longitude DOUBLE, latitude DOUBLE, elevation DOUBLE, velocity DOUBLE, heading DOUBLE, grade DOUBLE, fuel_consumption DOUBLE, nominal_fuel_consumption DOUBLE, timestamp TIMESTAMP) timestamp(timestamp) PARTITION BY DAY")
	//	if err != nil {
	//		panic(fmt.Errorf("fatal error, failed to create readings table: %s", err))
	//	}
	//
	//	// 创建 IoT diagnostics 表
	//	_, err = execQuery(questdbRESTEndPoint, "CREATE TABLE IF NOT EXISTS diagnostics (name SYMBOL, status DOUBLE, current_load DOUBLE, fuel_state DOUBLE, nominal_fuel_consumption DOUBLE, timestamp TIMESTAMP) timestamp(timestamp) PARTITION BY DAY")
	//	if err != nil {
	//		panic(fmt.Errorf("fatal error, failed to create diagnostics table: %s", err))
	//	}
	//}

	return tableExists
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	return nil
}

func (d *dbCreator) CreateDB(dbName string) error {
	time.Sleep(time.Second)
	return nil
}

type QueryResponseColumns struct {
	Name string
	Type string
}

type QueryResponse struct {
	Query   string
	Columns []QueryResponseColumns
	Dataset [][]interface{}
	Count   int
	Error   string
}

func execQuery(uriRoot string, query string) (QueryResponse, error) {
	var qr QueryResponse
	if strings.HasSuffix(uriRoot, "/") {
		uriRoot = uriRoot[:len(uriRoot)-1]
	}
	uriRoot = uriRoot + "/exec?query=" + url.QueryEscape(query)
	resp, err := http.Get(uriRoot)
	if err != nil {
		return qr, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return qr, err
	}
	err = json.Unmarshal(body, &qr)
	if err != nil {
		return qr, err
	}
	if qr.Error != "" {
		return qr, errors.New(qr.Error)
	}
	return qr, nil
}
