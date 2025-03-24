package meta

import (
	"context"
	"fmt"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"testing"
)

func TestMetaStore_ExecuteTxn(t *testing.T) {
	ms, err := NewMetaStore("10.0.11.101:2379")
	if err != nil {
		t.Fatal(err)
	}
	err = ms.ExecuteTxn(func(txn *transaction.KVTxn) error {
		pathKey := GetPathKey("T03", "test1")
		bytes, err := txn.Get(context.Background(), pathKey)
		if err != nil {
			return err
		}
		fmt.Println(string(bytes))
		fmt.Println("-----------------------------")
		objKey := GetObjKey("T03", string(bytes), "test")
		bytes, err = txn.Get(context.Background(), objKey)
		if err != nil {
			return err
		}
		fmt.Println(string(bytes))
		return nil
	})
	if err != nil {
		fmt.Println(err)
	}
}
