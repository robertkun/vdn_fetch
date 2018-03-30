package main

import (
	"./com"
	"./vconf"
	"./vmysql"
	"./worker"
	"time"
	"github.com/jinzhu/gorm"
	"log"
)

var gDmInfo = make([]*com.Domain, 10)
var gLastDms = make([]com.Domain_in_cdn_vendor, 10)

func DomainSliceEqual(new, old []com.Domain_in_cdn_vendor) bool {
	if len(new) != len(old) {
		return false
	}

	if (new == nil) != (old == nil) {
		return false
	}

	old = old[:len(new)]
	for i, v := range new {
		if v != old[i] {
			return false
		}
	}

	return true
}

func updateDomain(db *gorm.DB) {
	for ;; {
		var dms []com.Domain_in_cdn_vendor
		db.Find(&dms)
		if len(dms) <= 0 {
			log.Println("Get domain info failed! Continue!")
			time.Sleep(time.Second * 5)
			continue
		}

		if !DomainSliceEqual(dms, gLastDms) {
			gLastDms = make([]com.Domain_in_cdn_vendor, len(dms))
			copy(gLastDms, dms)

			gDmInfo = make([]*com.Domain, 0)
			for _, v := range dms {
				if v.Di_if_querydata == 1 && v.Di_if_del == 0 {
					var dm = new(com.Domain)
					dm.DmCId = v.Di_cust_id
					dm.DmDId = v.Di_dm_id
					dm.DmCdnId = v.Di_cm_id
					dm.DmDel = v.Di_if_del
					dm.DmQueryDelay = v.Di_query_delay
					dm.DmLastTm = 0
					dm.DmName = v.Di_dname

					gDmInfo = append(gDmInfo, dm)
				}
			}

			for _, v := range gDmInfo {
				log.Printf("dm: cid=%v, did=%v, name=%v, cdnid=%v, del=%v, delay=%v",
					v.DmCId, v.DmDId, v.DmName, v.DmCdnId, v.DmDel, v.DmQueryDelay)
			}
		}

		log.Println("Update domain info success.")
		time.Sleep(time.Minute * 1)
	}
}

func main() {
	vconf.LoadConfig("./config/config.toml")
	//time.Sleep(time.Minute)
	//vlog.InitLog("./log")
	//vhttp.InitServer()

	db := vmysql.GetConnDest("rhy_conf")
	if db == nil {
		log.Panic("db is nil.")
		return
	}

	defer db.Close()

	go updateDomain(db)

	worker.InitWorkerPool()
	worker.JobDispatch()

	t := time.NewTimer(time.Second * 1)
	var idx = 0
	for {
		select {
		case <-t.C:
			tmNow := time.Now().Unix()
			for _, v := range gDmInfo {
				duration := tmNow - v.DmLastTm

				if duration > int64(v.DmQueryDelay*60) {
					idx++
					if idx > 1000000 {
						idx = 1
					}

					v.DmLastTm = tmNow

					var job = new(com.JobInfo)
					job.JobId = idx
					job.CuId = v.DmCId
					job.DmId = v.DmDId
					job.CdnId = v.DmCdnId
					tm := (tmNow-int64(v.DmQueryDelay)*60)/300*300
					job.Tm = tm

					log.Printf("Job[%v] start. customer[%v], domain[%v], tm[%v].", idx, v.DmCId, v.DmDId, tm)
					time.Sleep(time.Second*1)
					worker.AddJob(job)
				}
			}

			t.Reset(time.Second * 1)
		}
	}
}
