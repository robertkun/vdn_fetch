package handle

import (
	"fmt"
	"log"
	"../com"
	"../vmysql"
	"github.com/jinzhu/gorm"
	"strings"
	"time"
)

var DetailUrl = "detail_url_flow"
var FlowS5 = "flow_s5"
var FlowV5 = "flow_v5"
var FlowVSip = "flow_v_sip"
var FlowSSip = "flow_s_sip"

func GetTableName(db *gorm.DB, dbname string, tbname string, tm int64) (tb string) {
	//db = vmysql.GetConnSource("information_schema")

	var res []com.TbName
	db.Raw("select table_name from information_schema.tables where table_schema=?", dbname).Scan(&res)

	if len(res) == 0 {
		log.Printf("Get source date table name failed! [%s]\n",
			fmt.Sprintf("select table_name from information_schema.tables where table_schema=%s", tbname))
		return
	}

	for _, v := range res {
		if strings.HasPrefix(v.Table_name, tbname) {
			var nameList []string
			nameList = strings.Split(v.Table_name, "_")

			start := com.DateToTm(nameList[len(nameList)-2])
			end := com.DateToTm(nameList[len(nameList)-1])

			if start < tm && tm <= end {
				tb = v.Table_name
				//fmt.Println(v.Table_name)
				//time.Sleep(time.Second)
			}
		}
	}

	if tb == "" {
		tb = tbname
	}

	return tb
}

func ProcessFlowV5(job *com.JobInfo) {
	funcName := "ProcessFlowV5"
	var sourceDbName = fmt.Sprintf("flowdb_%08d", job.CuId)
	sourceDb := vmysql.GetConnSource(sourceDbName)
	if sourceDb == nil {
		log.Panic(fmt.Sprintf("[%s] Get source db failed! sourceDb name=%s", funcName, sourceDbName))
		return
	}

	tb := GetTableName(sourceDb, sourceDbName, FlowVSip, job.Tm)
	if tb == "" {
		log.Printf("[%s] Get table name is empty! Return!", funcName)
		return
	}

	var res []com.Flow_v5
	sourceDb.Table(tb).Select("timestamp, date, dnid, sum(flow) as flow").
		Where("timestamp=? and dnid=?", job.Tm, job.DmId).Find(&res)

	//fmt.Println(res)
	if len(res) == 0 {
		log.Printf("[%s] Job[%v] return. Get Data is empty! customer:[%v], domain:[%v], tm:[%v]",
			funcName, job.JobId, job.CuId, job.DmId, job.Tm)
		return
	}

	// write data
	var rhyDbName = com.GetDbName(job.CuId)
	rhydb := vmysql.GetConnDest(rhyDbName)
	if rhydb == nil {
		log.Panic(fmt.Sprintf("[%s] Get rhydb failed! rhydb name=%s", funcName, rhyDbName))
		return
	}

	if !rhydb.HasTable(&com.Flow_v5{}) {
		rhydb.CreateTable(&com.Flow_v5{})
	}

	for _, v := range res {
		if v.Timestamp == 0 {
			continue
		}

		v.Cdn_id = job.CdnId
		v.Updated_timestamp = time.Now()
		rhydb.Create(&v)
		//fmt.Println(v)
	}

	log.Printf("Job[%v] process [flow_v5] over!", job.JobId)
}

func ProcessFlowS5(job *com.JobInfo) {
	funcName := "ProcessFlowS5"
	var sourceDbName = fmt.Sprintf("flowdb_%08d", job.CuId)
	sourceDb := vmysql.GetConnSource(sourceDbName)
	if sourceDb == nil {
		log.Panic(fmt.Sprintf("[%s] Get source db failed! sourceDb name=%s", funcName, sourceDbName))
		return
	}

	tb := GetTableName(sourceDb, sourceDbName, FlowSSip, job.Tm)
	if tb == "" {
		log.Printf("[%s] Get table name is empty! Return!", funcName)
		return
	}

	var res []com.Flow_s5
	sourceDb.Table(tb).Select("timestamp, date, dnid, sum(flow) as flow").
		Where("timestamp=? and dnid=?", job.Tm, job.DmId).Find(&res)

	if len(res) == 0 {
		log.Printf("[%s] Job[%v] return. Get Data is empty! customer:[%v], domain:[%v], tm:[%v]",
			funcName, job.JobId, job.CuId, job.DmId, job.Tm)
		return
	}

	// write data
	var rhyDbName = com.GetDbName(job.CuId)
	rhydb := vmysql.GetConnDest(rhyDbName)
	if rhydb == nil {
		log.Panic(fmt.Sprintf("[%s] Get rhydb failed! rhydb name=%s", funcName, rhyDbName))
		return
	}

	if !rhydb.HasTable(&com.Flow_s5{}) {
		rhydb.CreateTable(&com.Flow_s5{})
	}

	for _, v := range res {
		if v.Timestamp == 0 {
			continue
		}

		v.Cdn_id = job.CdnId
		v.Updated_timestamp = time.Now()
		rhydb.Create(&v)
		//fmt.Println(v)
	}

	log.Printf("Job[%v] process [flow_s5] over!", job.JobId)
}

func ProcessPIspFlowV5(job *com.JobInfo) {
	funcName := "ProcessPIspFlowV5"
	var sourceDbName = fmt.Sprintf("flowdb_%08d", job.CuId)
	sourceDb := vmysql.GetConnSource(sourceDbName)
	if sourceDb == nil {
		log.Panic(fmt.Sprintf("[%s] Get source db failed! sourceDb name=%s", funcName, sourceDbName))
		return
	}

	tb := GetTableName(sourceDb, sourceDbName, FlowV5, job.Tm)
	if tb == "" {
		log.Printf("[%s] Get table name is empty! Return!", funcName)
		return
	}

	var res []com.Pisp_flow_v5
	sourceDb.Table(tb).Select("timestamp, date, dnid, ispid, pid, sum(flow) as flow").
		Where("timestamp=? and dnid=?", job.Tm, job.DmId).Group("ispid, pid").Find(&res)

	if len(res) == 0 {
		log.Printf("[%s] Job[%v] return. Get Data is empty! customer:[%v], domain:[%v], tm:[%v]",
			funcName, job.JobId, job.CuId, job.DmId, job.Tm)
		return
	}

	// write data
	var rhyDbName = com.GetDbName(job.CuId)
	rhydb := vmysql.GetConnDest(rhyDbName)
	if rhydb == nil {
		log.Panic(fmt.Sprintf("[%s] Get rhydb failed! rhydb name=%s", funcName, rhyDbName))
		return
	}

	if !rhydb.HasTable(&com.Pisp_flow_v5{}) {
		rhydb.CreateTable(&com.Pisp_flow_v5{})
	}

	for _, v := range res {
		if v.Timestamp == 0 {
			continue
		}

		v.Cdn_id = job.CdnId
		v.Updated_timestamp = time.Now()
		rhydb.Create(&v)
		//fmt.Println(v)
	}

	log.Printf("Job[%v] process [pisp_flow_v5] over!", job.JobId)
}