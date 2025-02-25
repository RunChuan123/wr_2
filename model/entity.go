package model

import (
	"time"
	"wr_2/utils"
)

// 使用go的map结构实现的数据结构

type MapEntity struct {
	Entities map[int]*DataPair
}

func InitMap() *MapEntity {
	return &MapEntity{make(map[int]*DataPair)}
}

func (m *MapEntity) Len() int {
	return len(m.Entities)
}
func (m *MapEntity) GossipUpdate() []GossipUpdateData {
	var g []GossipUpdateData
	for _, v := range m.Entities {
		if v.Update {
			g = append(g, GossipUpdateData{Key: v.OriginKey, Value: v.Value, V: v.V})
			v.Update = false
		}
	}
	return g
}
func (m *MapEntity) Insert(id int, value interface{}, originKey string) {
	m.Entities[id] = &DataPair{OriginKey: originKey, Value: value, V: time.Now().UnixNano(), Update: true}
}
func (m *MapEntity) Delete(id int, key string) bool {
	if _, ok := m.Entities[id]; !ok {
		return false
	}
	delete(m.Entities, id)
	return true
}
func (m *MapEntity) Search(key string) (*DataPair, int, bool) {
	intKey := utils.ToHash(key)
	if _, ok := m.Entities[intKey]; !ok {
		return nil, -1, false
	}
	return m.Entities[intKey], -1, true

}
