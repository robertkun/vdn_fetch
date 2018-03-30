package vlog

import (
	"fmt"
	"log"
	"os"
	"time"
	"strings"
)

var normal_log string = ""
var error_log string = ""

func check_path_exist(fpath string) bool {
	var exist = true
	if _, err := os.Stat(fpath); os.IsNotExist(err) {
		exist = false
	}

	return exist
}

func InitLog(path string) {
	fmt.Println("logger init, logger path=", path)

	if exist := check_path_exist(path); !exist {
		fmt.Sprintf("Dir [%v] not existed!\n", path)

		// Make Dir
		err := os.Mkdir(path, os.ModePerm)
		if err != nil {
			fmt.Sprintf("mkdir failed![%v]\n", err)
			return
		} else {
			fmt.Sprintf("mkdir success!\n")
		}
	}

	strDate := time.Now().Format("2006-01-02")
	normal_log = fmt.Sprintf("%v/normal_%v.log", path, strDate)
	if exist := check_path_exist(normal_log); !exist {
		fmt.Sprintf("Log [%v] not existed! Create it!\n", normal_log)

		_, err := os.Create(normal_log)
		if err != nil {
			fmt.Sprintf("Create normal logger file failed![%v]\n", err)
			return
		}
	}

	error_log = fmt.Sprintf("%v/error_%v.log", path, strDate)
	if exist := check_path_exist(error_log); !exist {
		fmt.Sprintf("Log [%v] not existed! Create it!\n", error_log)

		_, err := os.Create(error_log)
		if err != nil {
			fmt.Sprintf("Create error logger file failed![%v]\n", err)
			return
		}
	}
}

func LogDef(msgs ... string) {
	var strMsg = ""
	for _, msg := range msgs {
		strMsg += msg + ","
	}
	strMsg = strings.TrimRight(strMsg, ",")
	fmt.Println("log:",strMsg, normal_log)

	logFile, err := os.OpenFile(normal_log, os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		fmt.Sprintf("Open normal logger file failed![%v]\n", err)
		return
	}

	logger := log.New(logFile, "", log.Ldate|log.Ltime|log.LstdFlags)
	logger.Println(strMsg)

	logFile.Close()
}

func LogErr(msgs ... string) {
	var strMsg = ""
	for _, msg := range msgs {
		strMsg += msg + ","
	}
	strMsg = strings.TrimRight(strMsg, ",")
	fmt.Println(strMsg)

	logFile, err := os.OpenFile(error_log, os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		fmt.Sprintf("Open error logger file failed![%v]\n", err)
		return
	}

	logger := log.New(logFile, "", log.Ldate|log.Ltime|log.LstdFlags)
	logger.Println(strMsg)

	logFile.Close()
}
