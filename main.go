package main

import (
	"context"
	"encoding/json"
	"flag"
	"github.com/billziss-gh/cgofuse/fuse"
	"log"
	"os"
	"time"
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
func autoRM() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if len(global.EmptyDirs) > 0 {
				for _, dir := range global.EmptyDirs {
					err := meta.GlobalTikv.RmdirAuto(dir)
					if err != nil {
						global.GlobalLogger.Printf("error to remove empty dir(%s),err:%v", dir, err)
					}
				}
				global.EmptyDirs = nil
			}
		}
	}
}

func main() {
	initLog()
	initGlobal()
	client := base.NewClient()
	fs := &base.MS{Client: client, Bucket: global.GlobalSetting.BucketName, MountPoint: global.GlobalSetting.MountPoint}
	go fs.ScanEmptyDirAuto(global.GlobalSetting.BucketName, context.Background())
	go autoRM()
	host := fuse.NewFileSystemHost(fs)
	str := []string{
		"-o", "allow_other",
		"-o", "name=minions",
		"-o", "uid=-1", // 设置匿名用户
		"-o", "gid=-1", // 设置匿名组
		"-o", "default_permissions", // 启用权限检查
		"-o", "attr_timeout=3", // 缓存时间3s
		"-o", "rw",
	}
	host.Mount(global.GlobalSetting.MountPoint, str)

}
