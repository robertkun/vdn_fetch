package vconf

import (
	"fmt"
	"encoding/base64"
	"github.com/BurntSushi/toml"
	"os"
	"log"
	"crypto/cipher"
	"crypto/aes"
	"bytes"
)

var gConf Config
var gUser string = "vdnCloud"
var aeskey = []byte("q3e023u9y8d2fwfl")

type Config struct {
	DB_IP_S string		// DB Ip
	DB_IP_D string		// DB Ip
	DB_PORT int			// DB Port
	DB_USER string 		// DB User
	DB_PASS string		// DB Pass
	DB_MAX_OPEN int		// DB Max Open
	DB_MAX_IDLE int		// DB Max Idle
	CDN_ID		int		// CDN ID
}

func PKCS5Padding(ciphertext []byte, blockSize int) []byte {
	padding := blockSize - len(ciphertext)%blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padtext...)
}

func PKCS5UnPadding(origData []byte) []byte {
	length := len(origData)
	unpadding := int(origData[length-1])
	return origData[:(length - unpadding)]
}

func AesEncrypt(origData, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	blockSize := block.BlockSize()
	origData = PKCS5Padding(origData, blockSize)
	blockMode := cipher.NewCBCEncrypter(block, key[:blockSize])
	crypted := make([]byte, len(origData))
	blockMode.CryptBlocks(crypted, origData)
	return crypted, nil
}

func AesDecrypt(crypted, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	blockSize := block.BlockSize()
	blockMode := cipher.NewCBCDecrypter(block, key[:blockSize])
	origData := make([]byte, len(crypted))
	blockMode.CryptBlocks(origData, crypted)
	origData = PKCS5UnPadding(origData)
	return origData, nil
}

// Check path exist
func IsPathExist(fpath string) bool {
	var exist = true
	if _, err := os.Stat(fpath); os.IsNotExist(err) {
		exist = false
	}

	return exist
}

func LoadConfig(cpath string) bool {
	if _, err := toml.DecodeFile(cpath, &gConf); err != nil {
		log.Println(err.Error())
		return false
	}

	bytesPass, err := base64.StdEncoding.DecodeString(gConf.DB_PASS)
	if err != nil {
		log.Println("Passwd base64.StdEncoding.DecodeString error!", err.Error())
		return false
	}

	pass, err := AesDecrypt(bytesPass, aeskey)
	if err != nil {
		log.Println("Passwd AesDecrypt error!", err.Error())
		return false
	} else {
		gConf.DB_PASS = string(pass)
	}

	log.Println(fmt.Sprintf("DB_IP_S=%v, DB_IP_D=%v, DB_PORT=%v, DB_USER=%v, DB_MAX_OPEN=%v, DB_MAX_IDLE=%v",
		gConf.DB_IP_S, gConf.DB_IP_D, gConf.DB_PORT, gConf.DB_USER, gConf.DB_MAX_OPEN, gConf.DB_MAX_IDLE))

	log.Printf("CDN_ID=%v", gConf.CDN_ID)
	return true
}

func DBIpS() string {
	return gConf.DB_IP_S
}

func DBIpD() string {
	return gConf.DB_IP_D
}

func DBPort() int {
	return gConf.DB_PORT
}

func DBUser() string {
	return gConf.DB_USER
}

func DBPass() string {
	return gConf.DB_PASS
}

func DBMaxOpen() int {
	return gConf.DB_MAX_OPEN
}

func DBMaxIdle() int {
	return gConf.DB_MAX_IDLE
}

func CDNID() int {
	return gConf.CDN_ID
}