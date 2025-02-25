package model

import (
	"sync"
	"time"
)

// 数据结构，实现了两种，一种是B+树，在代码中使用这个，一种是go的map
type DataStruct interface {
	Insert(intKey int, value interface{}, key string)
	Delete(intKey int, key string) bool
	Search(key string) (*DataPair, int, bool)
	Len() int
	GossipUpdate() []GossipUpdateData
}

// 节点使用的结构体，V是版本好，用纳秒时间戳来表示，Update表示是否需要更新，只有需要更新且时间戳更新才会更新数据
type DataPair struct {
	OriginKey string
	Value     interface{}
	Mu        sync.RWMutex
	V         int64
	Update    bool
	CreatedAt time.Time
}

// 用于gossip传播的结构体
type GossipUpdateData struct {
	Key   string
	Value interface{}
	V     int64
}
type GossipAllData struct {
	Update []GossipUpdateData
	Delete []string
}

// 过期删除结构体
type ExpiredData struct {
	Key       string
	CreatedAt time.Time
}
