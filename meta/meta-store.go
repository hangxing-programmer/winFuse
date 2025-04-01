package meta

import (
	"context"
	"fmt"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"strings"
	"time"
	"winFuse/global"
)

var (
	GlobalTikv *MetaStore
	OS         = "OS"
	SLASH      = "/"
	BUCKET     = "Bucket"
	LIST       = "List"
	NAME       = "Name"
	DATA       = "Data"
	LS         = "LS"
)

type MetaStore struct {
	tikvAddr           string
	defaultTikvTimeOut time.Duration
	txnClient          *txnkv.Client
}

func NewMetaStore(tikvAddr string) (*MetaStore, error) {
	tikvAddrs := strings.Split(tikvAddr, ",")
	client, err := txnkv.NewClient(tikvAddrs)
	if err != nil {
		global.GlobalLogger.Printf("NewMetaStore err:%v", err)
		return nil, err
	}

	ms := &MetaStore{
		txnClient:          client,
		defaultTikvTimeOut: time.Second * 60,
	}
	return ms, nil
}

// OS/T03/Bucket/List/Name/test1
func GetPathKey(tenantId, bucket string) []byte {
	return []byte(OS + SLASH + tenantId + SLASH + BUCKET + SLASH + LIST + SLASH + NAME + SLASH + bucket)
}

// OS/T03/123456789987/Data/LS/test/
func GetObjKey(tenantID, pathKey, obj string) []byte {
	return []byte(OS + SLASH + tenantID + SLASH + pathKey + SLASH + DATA + SLASH + LS + SLASH + obj + SLASH)
}

// 事务执行方法
func (ms *MetaStore) ExecuteTxn(fn func(txn *transaction.KVTxn) error) error {
	txn, err := ms.txnClient.Begin()
	if err != nil {
		return fmt.Errorf("事务启动失败: %w", err)
	}
	if err := fn(txn); err != nil {
		_ = txn.Rollback()
		return err
	}
	if err := txn.Commit(context.Background()); err != nil {
		return fmt.Errorf("事务提交失败: %w", err)
	}
	return nil
}

// 删除空文件夹
func (ms *MetaStore) RmdirAuto(path string) error {
	ctx := context.Background()
	dirName := strings.TrimSuffix(path, "/")

	err := ms.ExecuteTxn(func(txn *transaction.KVTxn) error {
		pathKey := GetPathKey(global.GlobalSetting.TenantID, global.GlobalSetting.BucketName)
		num, err := txn.Get(ctx, pathKey)
		if err != nil {
			return err
		}
		objKey := GetObjKey(global.GlobalSetting.TenantID, string(num), dirName)
		err = txn.Delete(objKey)
		fmt.Printf("object = %s has been deleted", string(objKey))
		return err
	})
	return err
}
