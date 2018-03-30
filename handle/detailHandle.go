package handle

import (
	"../com"
	"../vmysql"
	"fmt"
	"log"
	"time"
)

func ProcessDetail(job *com.JobInfo) {
	funcName := "ProcessDetail"
	var sourceDbName = fmt.Sprintf("detail_log_%08d", job.CuId)
	sourcedb := vmysql.GetConnSource(sourceDbName)
	if sourcedb == nil {
		log.Printf("[%s] Get source db failed! sourcedb name=%s", funcName, sourceDbName)
		return
	}

	tb := GetTableName(sourcedb, sourceDbName, DetailUrl, job.Tm)
	if tb == "" {
		log.Printf("[%s] Get table name is empty! Return!", funcName)
		return
	}

	date := com.TmToDate(job.Tm)
	var res []com.Top_url
	sourcedb.Table(tb).Select("d_date as date, d_dnid as dnid, md5(d_url) as urlmd5, " +
		"d_url as url, sum(d_count) as pv, sum(d_flow) as flow").Where("d_date=? and d_dnid=?",
			date, job.DmId).Group("d_url").Order("pv desc").Limit(1000).Find(&res)

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

	if !rhydb.HasTable(&com.Top_url{}) {
		rhydb.CreateTable(&com.Top_url{})
	}

	rhydb.Where("date=? and cdn_id=?", date, job.CdnId).Delete(&com.Top_url{})
	for _, v := range res {
		if v.Date == 0 {
			continue
		}

		v.Cdn_id = job.CdnId
		v.Updated_timestamp = time.Now()
		rhydb.Create(&v)
		//fmt.Println(v)
	}

	log.Printf("Job[%v] process [top_url] over!", job.JobId)
}
