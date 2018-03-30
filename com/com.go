package com

import (
	"time"
	"fmt"
)

var TimeFormat = "2006-01-02 15:04:05"
var DBPerfix = "rhydb_"

type Domain_in_cdn_vendor struct {
	Di_dm_id		int
	Di_cust_id		int
	Di_cm_id		int
	Di_if_querydata	int
	Di_query_delay	int
	Di_if_getlog	int
	Di_getlog_delay	int
	Di_getlog_plog	int
	Di_if_del		int
	Di_one_min		int
	Di_dname		string
}

type Domain struct {
	DmCId   		int
	DmDId			int
	DmCdnId			int
	DmDel   		int
	DmOneMin   		int
	DmQueryDelay 	int
	DmLastTm  		int64
	DmName  		string
}

type JobInfo struct {
	JobId			int
	CuId			int
	DmId			int
	CdnId			int
	Tm				int64
}

type Flow_v5 struct {
	ID				int
	Timestamp		int64			`gorm:"index:timestamp"`
	Date			int64			`gorm:"index:date"`
	Cdn_id			int				`gorm:"index:cdn_id"`
	Dnid			int				`gorm:"index:dnid"`
	Flow			int64			`gorm:"default:0"`
	Updated_timestamp time.Time
}

type Flow_s5 struct {
	ID				int
	Timestamp		int64			`gorm:"index:timestamp"`
	Date			int64			`gorm:"index:date"`
	Cdn_id			int				`gorm:"index:cdn_id"`
	Dnid			int				`gorm:"index:dnid"`
	Flow			int64			`gorm:"default:0"`
	Updated_timestamp time.Time
}

type Pisp_flow_v5 struct {
	ID				int
	Timestamp		int64			`gorm:"index:timestamp"`
	Date			int64			`gorm:"index:date"`
	Cdn_id			int				`gorm:"index:cdn_id"`
	Dnid			int				`gorm:"index:dnid"`
	Ispid			int				`gorm:"index:ispid"`
	Pid				int				`gorm:"index:pid"`
	Cid				int
	Flow			int64			`gorm:"default:0"`
	Updated_timestamp time.Time
}

type Pv_v5 struct {
	ID				int
	Timestamp		int64			`gorm:"index:timestamp"`
	Date			int64			`gorm:"index:date"`
	Cdn_id			int				`gorm:"index:cdn_id"`
	Dnid			int				`gorm:"index:dnid"`
	Pv				int64
	Type			int
	Updated_timestamp time.Time
}

type Pv_s5 struct {
	ID				int
	Timestamp		int64			`gorm:"index:timestamp"`
	Date			int64			`gorm:"index:date"`
	Cdn_id			int				`gorm:"index:cdn_id"`
	Dnid			int				`gorm:"index:dnid"`
	Pv				int64
	Type			int
	Updated_timestamp time.Time
}

type Rescode_v5 struct {
	ID				int
	Timestamp		int64			`gorm:"index:timestamp"`
	Date			int64			`gorm:"index:date"`
	Cdn_id			int				`gorm:"index:cdn_id"`
	Dnid			int				`gorm:"index:dnid"`
	Rescode			int				`gorm:"index:rescode"`
	Pv				int64
	Flow			int64
	Updated_timestamp time.Time
}

type Rescode_s5 struct {
	ID				int
	Timestamp		int64			`gorm:"index:timestamp"`
	Date			int64			`gorm:"index:date"`
	Cdn_id			int				`gorm:"index:cdn_id"`
	Dnid			int				`gorm:"index:dnid"`
	Rescode			int				`gorm:"index:rescode"`
	Pv				int64
	Flow			int64
	Updated_timestamp time.Time
}

type Top_url struct {
	ID				int
	Date			int64			`gorm:"index:date"`
	Cdn_id			int				`gorm:"index:cdn_id"`
	Dnid			int				`gorm:"index:dnid"`
	Urlmd5			string			`gorm:"index:urlmd5"`
	Url				string
	Pv				int64
	Flow			int64
	Updated_timestamp time.Time
}

type TbName struct {
	Table_name string
}

func DateToTm(toBeChange string) int64 {
	timeLayout := "20060102"
	loc, _ := time.LoadLocation("Local")
	theTime, _ := time.ParseInLocation(timeLayout, toBeChange, loc)
	tm := theTime.Unix()

	return tm
}

func TmToDate(tm int64) string {
	timeLayout := "20060102"
	dataTimeStr := time.Unix(tm, 0).Format(timeLayout)
	return dataTimeStr
}

func GetDbName(cuid int) string {
	var rhyDbName = fmt.Sprintf("%s%08d", DBPerfix, cuid)
	return rhyDbName
}