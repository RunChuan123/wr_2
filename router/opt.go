// gin路由主要逻辑代码

package router

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"net/http"
	"sync"
	"time"
	"wr_2/model"
	"wr_2/utils"
)

// 数据结构初始化
var m = InitStruct()

func InitStruct() model.DataStruct {
	// config文件读取需要使用的数据结构
	s, ok := utils.ReadKey("dataStruct")
	if !ok {
		return nil
	}
	var dataStruct model.DataStruct
	switch s {
	case "BPTree":
		dataStruct = model.NewTree()
	case "Map":
		dataStruct = model.InitMap()
	}
	sqlData := utils.Start()
	for _, data := range sqlData {
		fmt.Println(data)
		dataStruct.Insert(utils.ToHash(data.Key), data.Value, data.Key)
	}
	return dataStruct
}

// 全局锁 在普通crud操作中使用读锁，在gossip集中更新时使用写锁
var globalMutex sync.RWMutex

// 新增或更新数据
func InsertAndUpdate(c *gin.Context) {
	var body map[string]interface{}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	var k string
	for keys := range body {
		k = keys
		break
	}
	if k == "" {
		c.JSON(400, gin.H{"error": "keys is empty"})
	}
	data, _ := body[k]

	globalMutex.RLock()
	defer globalMutex.RUnlock()
	d, _, ok := m.Search(k)
	if ok == false {
		// 不存在就插入
		m.Insert(utils.ToHash(k), data, k)
		c.JSON(200, gin.H{"message": "insert success"})
	} else {
		// 存在就先加记录锁，再更新数据
		(*d).Mu.Lock()
		d.Value = data
		d.V = time.Now().UnixNano()
		(*d).Mu.Unlock()
		c.JSON(200, gin.H{"message": "update success"})
	}

}

// 查询数据
func Search(c *gin.Context) {
	key := c.Query("key")
	fmt.Println(key)
	globalMutex.RLock()
	defer globalMutex.RUnlock()
	data, _, ok := m.Search(key)
	if !ok {
		c.JSON(404, gin.H{"message": "key not found"})
		return
	}
	expirationData <- model.ExpiredData{Key: key, CreatedAt: data.CreatedAt}
	c.JSON(200, gin.H{"data": data.Value})

}

// 删除数据
func Delete(c *gin.Context) {
	key := c.Query("key")
	globalMutex.RLock()
	defer globalMutex.RUnlock()
	d, _, ok := m.Search(key)
	if !ok {
		c.JSON(404, gin.H{"message": "key not found"})
	}

	// 先加锁再删除
	d.Mu.Lock()
	m.Delete(utils.ToHash(key), key)
	d.Mu.Unlock()
	c.JSON(200, gin.H{"message": "delete success"})
}

func Count(c *gin.Context) {
	c.JSON(200, gin.H{"total": m.Len()})

}

// gossip接受并更新数据
func GossipRecv(c *gin.Context) {
	var receData model.GossipAllData
	if err := c.ShouldBindJSON(&receData); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	globalMutex.Lock()
	defer globalMutex.Unlock()
	for _, data := range receData.Update {
		localData, _, exists := m.Search(data.Key)
		if exists {
			if localData.V < data.V {
				localData.Mu.Lock()
				localData.V = data.V
				localData.Value = data.Value
				localData.Mu.Unlock()
			}
		} else {
			m.Insert(utils.ToHash(data.Key), data.Value, data.Key)
		}
	}
	for _, d := range receData.Delete {
		m.Delete(utils.ToHash(d), d)
	}
}

// 确定gossip消息发送频率
func HandleGossip(port string) {
	t := time.NewTicker(10 * time.Second)
	for _ = range t.C {
		GossipSend(port)
	}
}

// 发送gossip消息
func GossipSend(port string) {
	if m.Len() == 0 {
		return
	}
	globalMutex.RLock()
	defer globalMutex.RUnlock()
	var nodes = []string{
		"8080",
		"8081",
		"8082",
	}
	// 除去本节点
	for i, v := range nodes {
		if v == port {
			nodes = append(nodes[:i], nodes[i+1:]...)
			break
		}
	}
	// 需要发送的部分更新数据
	gQueue := m.GossipUpdate()
	var deleteKeys []string
	for {
		select {
		case key := <-gossipExpire:
			deleteKeys = append(deleteKeys, key)
		default:
			goto end
		}
	}
end:
	sendData := model.GossipAllData{Update: gQueue, Delete: deleteKeys}
	for _, node := range nodes {
		jsonData, err := json.Marshal(sendData)
		if err != nil {
			fmt.Println(err)
			return
		}
		// 发送到其他节点
		resp, err := http.Post("http://127.0.0.1:"+node+"/gossip/recv", "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			fmt.Println(err)
			return
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			fmt.Println("Failed to send gossip message to node: ")
			return
		}
	}
}

var expirationData = make(chan model.ExpiredData, 1000)
var gossipExpire = make(chan string, 1000)

func ExpirationMonitor() {
	for e := range expirationData {
		if time.Since(e.CreatedAt) > 5*time.Second {
			if _, _, ok := m.Search(e.Key); ok == false {
				continue
			}
			m.Delete(utils.ToHash(e.Key), e.Key)
			gossipExpire <- e.Key
		}
	}

}
