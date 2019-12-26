package gohbase

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/zhuyaoyhj/gohbase/hrpc"
	"log"
	"testing"
)

const (
	DefaultZkQuorum        = `rsync.master001.sunshine.hadoop.js.ted,rsync.master002.sunshine.hadoop.js.ted,rsync.master003.sunshine.hadoop.js.ted,rsync.master004.sunshine.hadoop.js.ted,rsync.master005.sunshine.hadoop.js.ted,rsync.master006.sunshine.hadoop.js.ted,rsync.master007.sunshine.hadoop.js.ted`
	DefaultZkRoot          = `/hbase2`
	DefaultResolutionTable = `wenwen:resolution_table_test3`
	DefaultEntityTable     = `wenwen:release_entities`
	DefaultMetaTable       = `wenwen:release_entities_metainfo`
)

var FamilyResolution = map[string][]string{"data": {"#resolution_id"}}

func GetSourceKey(sourceName, sourceID interface{}) string {
	tmpSlice := make([]interface{}, 2)
	tmpSlice[0] = sourceName
	tmpSlice[1] = sourceID
	tmpJson, _ := json.Marshal(tmpSlice)

	return Md5sum(tmpJson)
}

func Md5sum(str []byte) string {
	h := md5.New()
	h.Write(str)
	return hex.EncodeToString(h.Sum(nil))
}

func TestClient_Multi(t *testing.T) {
	client := NewClient(DefaultZkQuorum, ZookeeperRoot(DefaultZkRoot))
	req, err := hrpc.NewPutStr(context.Background(), DefaultResolutionTable, "1eacf94607f5ddba6b27842ef9530218", nil)
	req.SetSkipBatch(true)
	var temp hrpc.Call = req
	b, ok := temp.(hrpc.Batchable)
	if ok {
		fmt.Println("batchable", ok, b.SkipBatch())
	} else {
		fmt.Println("batchable", ok)
	}

	return
	//MultiRequest := hrpc.NewMultiStr()
	MultiRequest := hrpc.NewMulti(10)
	sourceKey := GetSourceKey("sogou_baike", "50854")
	fmt.Println(sourceKey)
	values := make(map[string]map[string][]byte)
	values["data"] = make(map[string][]byte)
	var value = []string{"test", "manual", "2019-11-21 16:18:44"}
	what, err := json.Marshal(value)
	values["data"]["baidu_baike"] = what
	MultiRequest.Add(context.Background(), DefaultResolutionTable, "1eacf94607f5ddba6b27842ef9530218", nil, "get")
	MultiRequest.Add(context.Background(), DefaultResolutionTable, "1eacf94607f5ddba6b27842ef9530218", values, "put")
	values = make(map[string]map[string][]byte)
	values["data"] = make(map[string][]byte)
	values["data"]["enwikidata"] = nil

	MultiRequest.Add(context.Background(), DefaultResolutionTable, "1eacf94607f5ddba6b27842ef9530218", values, "del")

	fmt.Println("Multi")
	getRsp, err := client.Multi(MultiRequest)
	fmt.Println("result")
	if err != nil {
		fmt.Println(err)
		log.Fatal()
	} else {
		/*result := make(map[string]interface{})
		//fmt.Println(getRsp.Cells)
		for _, item := range getRsp.Cells {
			if string(item.Family) == "data" {
				key := string(item.Qualifier)
				if key == "#resolution_id" {
					result[key] = string(item.Value)
				} else {
					var ids []interface{}
					json.Unmarshal(item.Value, &ids)
					result[key] = ids
				}
			}
		}*/
	}
	fmt.Println("final result", getRsp)

}

func TestClient_Get(t *testing.T) {
	client := NewClient(DefaultZkQuorum, ZookeeperRoot(DefaultZkRoot) /*, FlushInterval(1*time.Second)*/)
	sourceKey := GetSourceKey("sogou_baike", "50854")
	GetRequest, err := hrpc.NewGetStr(context.Background(), DefaultResolutionTable, sourceKey)
	GetRequest.SetSkipBatch(false)
	if err != nil {
		fmt.Println(err)
		return
	}

	getRsp, err := client.Get(GetRequest)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(getRsp)
}

func TestClient_Get2(t *testing.T) {
	for i := 0; i < 2; i++ {
		TestClient_Get(t)
	}
}
