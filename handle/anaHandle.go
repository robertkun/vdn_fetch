package handle

import (
	"fmt"
	"log"
	"time"
	"../com"
	"../vmysql"
)

func ProcessPvV5(job *com.JobInfo) {
	funcName := "ProcessPvS5"
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

	var res []com.Pv_v5
	sourceDb.Table(tb).Select("timestamp, date, dnid, sum(count) as pv, rescode as type").
		Where("timestamp=? and dnid=?", job.Tm, job.DmId).Group("rescode").Find(&res)

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

	if !rhydb.HasTable(&com.Pv_v5{}) {
		rhydb.CreateTable(&com.Pv_v5{})
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

	log.Printf("Job[%v] process [pv_v5] over!", job.JobId)
}

func ProcessPvS5(job *com.JobInfo) {
	funcName := "ProcessPvS5"
	var sourceDbName = fmt.Sprintf("flowdb_%08d", job.CuId)
	sourceDb := vmysql.GetConnSource(sourceDbName)
	if sourceDb == nil {
		log.Panic(fmt.Sprintf("[%s] Get source db failed! sourceDb name=%s", funcName, sourceDbName))
		return
	}

	tb := GetTableName(sourceDb, sourceDbName, FlowS5, job.Tm)
	if tb == "" {
		log.Printf("[%s] Get table name is empty! Return!", funcName)
		return
	}

	var res []com.Pv_s5
	sourceDb.Table(tb).Select("timestamp, date, dnid, sum(count) as pv, rescode as type").
		Where("timestamp=? and dnid=?", job.Tm, job.DmId).Group("rescode").Find(&res)

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

	if !rhydb.HasTable(&com.Pv_s5{}) {
		rhydb.CreateTable(&com.Pv_s5{})
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

	log.Printf("Job[%v] process [pv_s5] over!", job.JobId)
}

func ProcessRescodeV5(job *com.JobInfo) {
	funcName := "ProcessRescodeV5"
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

	var res []com.Rescode_v5
	sourceDb.Table(tb).Select("timestamp, date, dnid, rescode, sum(count) as pv, sum(flow) as flow").
		Where("timestamp=? and dnid=?", job.Tm, job.DmId).Group("rescode").Find(&res)

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

	if !rhydb.HasTable(&com.Rescode_v5{}) {
		rhydb.CreateTable(&com.Rescode_v5{})
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

	log.Printf("Job[%v] process [rescode_v5] over!", job.JobId)
}

func ProcessRescodeS5(job *com.JobInfo) {
	funcName := "ProcessRescodeS5"
	var sourceDbName = fmt.Sprintf("flowdb_%08d", job.CuId)
	sourceDb := vmysql.GetConnSource(sourceDbName)
	if sourceDb == nil {
		log.Panic(fmt.Sprintf("[%s] Get source db failed! sourceDb name=%s", funcName, sourceDbName))
		return
	}

	tb := GetTableName(sourceDb, sourceDbName, FlowS5, job.Tm)
	if tb == "" {
		log.Printf("[%s] Get table name is empty! Return!", funcName)
		return
	}

	var res []com.Rescode_s5
	sourceDb.Table(tb).Select("timestamp, date, dnid, rescode, sum(count) as pv, sum(flow) as flow").
		Where("timestamp=? and dnid=?", job.Tm, job.DmId).Group("rescode").Find(&res)

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

	if !rhydb.HasTable(&com.Rescode_s5{}) {
		rhydb.CreateTable(&com.Rescode_s5{})
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

	log.Printf("Job[%v] process [rescode_s5] over!", job.JobId)
}

