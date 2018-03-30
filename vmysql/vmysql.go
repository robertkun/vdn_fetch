package vmysql

import (
	"fmt"
	"../vconf"
	"github.com/jinzhu/gorm"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"sync"
)

var m = new(sync.Mutex)
var gSqlMap = make(map[string]*gorm.DB)

func GetConnDest(dbName string) (db *gorm.DB) {
	m.Lock()
	if db, ok := gSqlMap[dbName]; !ok {
		var host = vconf.DBIpD()
		var user = vconf.DBUser()
		var passwd = vconf.DBPass()
		var port = vconf.DBPort()

		db, err := gorm.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%v)/%s?charset=utf8&parseTime=True&loc=Local", user, passwd, host, port, dbName))
		db.SingularTable(true)
		db.DB().SetMaxIdleConns(vconf.DBMaxIdle())
		db.DB().SetMaxOpenConns(vconf.DBMaxOpen())

		if err != nil {
			log.Println(err)
			var defName = "rhy_conf"
			db, err := gorm.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%v)/%s?charset=utf8&parseTime=True&loc=Local", user, passwd, host, port, defName))
			if err != nil {
				log.Panic(err)
			} else {
				db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s DEFAULT CHARSET utf8 COLLATE utf8_general_ci;", dbName))
				db, err := gorm.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%v)/%s?charset=utf8&parseTime=True&loc=Local", user, passwd, host, port, dbName))
				if err != nil {
					log.Panic(err)
				} else {
					db.SingularTable(true)
					db.DB().SetMaxIdleConns(vconf.DBMaxIdle())
					db.DB().SetMaxOpenConns(vconf.DBMaxOpen())

					m.Unlock()
					return db
				}
			}

			m.Unlock()
			return nil
		} else {
			gSqlMap[dbName] = db
		}

		m.Unlock()
		return db
	} else {
		m.Unlock()
		return db
	}
}

func GetConnSource(dbName string) (db *gorm.DB) {
	m.Lock()
	if db, ok := gSqlMap[dbName]; !ok {
		var host = vconf.DBIpS()
		var user = vconf.DBUser()
		var passwd = vconf.DBPass()
		var port = vconf.DBPort()

		db, err := gorm.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%v)/%s?charset=utf8&parseTime=True&loc=Local", user, passwd, host, port, dbName))
		db.SingularTable(true)
		db.DB().SetMaxIdleConns(vconf.DBMaxIdle())
		db.DB().SetMaxOpenConns(vconf.DBMaxOpen())

		if err != nil {
			log.Println(err)
			m.Unlock()
			return nil
		} else {
			gSqlMap[dbName] = db
		}

		m.Unlock()
		return db
	} else {
		m.Unlock()
		return db
	}
}