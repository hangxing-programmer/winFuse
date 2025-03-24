package global

import (
	"log"
)

type GlobalSettingStruct struct {
	MountPoint string `json:"mountpoint"`
	Cache      string `json:"cache"`
	BucketName string `json:"bucketname"`
	Endpoint   string `json:"endpoint"`
	AccessKey  string `json:"accesskeyID"`
	SecretKey  string `json:"secretAccessKey"`
	UseSSL     bool   `json:"useSSL"`
	TikvAddr   string `json:"tikvAddr"`
	TenantID   string
}

var (
	GlobalSetting *GlobalSettingStruct
	GlobalLogger  *log.Logger
	EmptyDirs     = []string{}
)
