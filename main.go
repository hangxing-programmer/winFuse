package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"winFuse/base"
	"winFuse/global"
	"winFuse/meta"
)

func initLog() {
	global.GlobalLogger = log.New(os.Stdout, "", log.Lmsgprefix|log.LstdFlags)
}
func initGlobal() {
	str := flag.String("conf", "", "path")
	tenantID := flag.String("tenantID", "", "tenantID")
	flag.Parse()
	file, err := os.ReadFile(*str)
	if err != nil {
		log.Fatalln("error to open file", err)
		return
	}
	err = json.Unmarshal(file, &global.GlobalSetting)
	if err != nil {
		log.Fatalln("error to parse json", err)
		return
	}
	global.GlobalSetting.TenantID = *tenantID
	meta.GlobalTikv, err = meta.NewMetaStore(global.GlobalSetting.TikvAddr)
	if err != nil {
		log.Fatalln("error to create meta client", err)
	}
}

func main() {
	initLog()
	initGlobal()
	base.NewMount()
}
