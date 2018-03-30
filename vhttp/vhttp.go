package vhttp

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"net/http"
	"../vlog"
)

func response_success(c *gin.Context, msg string) {
	c.JSON(http.StatusOK, gin.H{
		"status": gin.H{
			"code":  http.StatusOK,
			"message": msg,
		},
	})

	vlog.LogDef(msg)
}

func response_error(c *gin.Context, code int, err error) {
	c.JSON(http.StatusOK, gin.H{
		"status": gin.H{
			"code":  code,
			"message": err.Error(),
		},
	})

	vlog.LogDef(err.Error())
}

func process_update(c *gin.Context) {
	bytes, err := c.GetRawData()
	if err != nil {
		response_error(c, 1001, err)
		return
	}
	data := string(bytes)

	var user string
	value := gjson.Get(data, "user")
	if value.Index != 0 {
		user = value.String()
		vlog.LogDef(user)
	} else {
		response_error(c, 1002, fmt.Errorf("Error:User can not be empty!"))
		return
	}

	response_success(c, "update success!")
}

func InitServer() {
	fmt.Println("Init Http Router")
	router := gin.Default()

	router.POST("/vfetch/update", func(c *gin.Context) {
		process_update(c)
	})

	router.Run(":80")
}
