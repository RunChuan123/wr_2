package model

import (
	"fmt"
	"sync"
	"time"
	"wr_2/utils"
)

// 使用B+树实现的数据结构

func NewTree() *Tree {
	return &Tree{
		root:     nil,
		Order:    4,
		MaxLen:   4,
		LeastLen: 2,
	}
}

// 键是相应子节点的最大值
// 每个key对应一个node
// 节点的数据结构，其中key是存入数据的key的hash转换后的int值，value存放了原始key字符串和value
type Node struct {
	Key      []int
	Value    [][]*DataPair // 每个叶子节点的每个key对应一个DataPair数组，其中存放hash冲突的多个数据，通常存放一个数据
	Children []*Node
	Parent   *Node
	IsLeaf   bool
	Next     *Node
	mu       sync.RWMutex
}

// 树的结构 First指向第一个叶节点
type Tree struct {
	root     *Node
	Order    int
	First    *Node
	MaxLen   int
	LeastLen int
}

// 初始化First指向
func (t *Tree) needFirst() bool {
	if t.root == nil {
		return false
	}
	current := t.root
	for !current.IsLeaf {
		current = current.Children[0]
	}
	t.First = current
	return true
}

// 寻找需要gossip传播的数据
func (t *Tree) GossipUpdate() []GossipUpdateData {

	success := t.needFirst()
	if !success {
		return []GossipUpdateData{}
	}
	first := t.First
	var g []GossipUpdateData
	for first != nil {
		for _, q := range first.Value {
			for _, d := range q {
				if d.Update {
					g = append(g, GossipUpdateData{Key: d.OriginKey, Value: d.Value, V: d.V})
					d.Update = false
				}
			}
		}
		first = first.Next
	}
	return g
}

func (t *Tree) Len() int {
	if t.root == nil {
		return 0
	}
	t.needFirst()
	first := t.First
	leng := 0
	for first != nil {
		for _, q := range first.Value {
			leng += len(q)
		}
		first = first.Next
	}
	return leng
}

// 删除操作，key是originKey的hash值
func (t *Tree) Delete(key int, originKey string) bool {
	if t.root == nil {
		return false
	}
	if len(t.root.Key) == 1 && t.root.Key[0] == key {
		t.root = nil
		return true
	}
	// 找到叶子节点
	leaf := t.findLeafNode(key)

	leafIndex := t.findIndex(leaf.Key, key)

	//找不到
	if leafIndex >= len(leaf.Key) || leaf.Key[leafIndex] != key {
		return false
	}

	//删除hash冲突时放在一起的数据
	if len(leaf.Value[leafIndex]) > 1 {
		_, index, exist := t.Search(originKey)
		if exist {
			leaf.Value[leafIndex] = append(leaf.Value[leafIndex][:index], leaf.Value[leafIndex][index+1:]...)
		}
	}
	// 如果删除叶子结点里面的最大值，需要更新父节点的key
	if key == leaf.Key[len(leaf.Key)-1] {
		changeKey := leaf.Key[len(leaf.Key)-2]
		current := leaf.Parent
		for current != nil {
			sign := false
			for i, k := range current.Key {
				if k == key {
					current.Key[i] = changeKey
					sign = true
					break
				}
			}
			if !sign {
				break
			}
			current = current.Parent
		}
	}
	//正常删除操作
	leaf.Key = append(leaf.Key[:leafIndex], leaf.Key[leafIndex+1:]...)
	leaf.Value = append(leaf.Value[:leafIndex], leaf.Value[leafIndex+1:]...)

	//如果小于最小长度，需要平衡叶子节点
	if len(leaf.Value) < t.LeastLen {
		t.balanceLeafNode(leaf)
	}
	return true
}

func (t *Tree) balanceLeafNode(leaf *Node) {
	if leaf.Parent == nil {
		return
	}
	parent := leaf.Parent
	index := t.getNodeIndex(parent, leaf)
	// 向左兄弟借
	if index > 0 {
		leftSibling := parent.Children[index-1]
		if len(leftSibling.Key) > t.LeastLen {
			leaf.Key = t.insertSlice(leaf.Key, 0, leftSibling.Key[len(leftSibling.Key)-1])
			leaf.Value = t.insertValueSlice(leaf.Value, 0, leftSibling.Value[len(leftSibling.Value)-1])
			leftSibling.Key = leftSibling.Key[:len(leftSibling.Key)-1]
			leftSibling.Value = leftSibling.Value[:len(leftSibling.Value)-1]
			parent.Key[index-1] = leftSibling.Key[len(leftSibling.Key)-1]
			return
		}
	}
	//向右兄弟借
	if index < len(parent.Key)-1 {
		rightSibling := parent.Children[index+1]
		if len(rightSibling.Key) > t.LeastLen {
			leaf.Key = append(leaf.Key, rightSibling.Key[0])
			leaf.Value = append(leaf.Value, rightSibling.Value[0])
			rightSibling.Key = rightSibling.Key[1:]
			rightSibling.Value = rightSibling.Value[1:]
			parent.Key[index] = leaf.Key[len(leaf.Key)-1]
			return
		}
	}
	// 合并节点 将本节点并入左兄弟
	if index > 0 {
		leftSibling := parent.Children[index-1]
		leftSibling.Key = append(leftSibling.Key, leaf.Key...)
		leftSibling.Value = append(leftSibling.Value, leaf.Value...)
		leftSibling.Next = leaf.Next
		//删除本节点
		t.deleteFromParent(parent, index-1, index)
	} else {
		//将右兄弟并入本节点
		rightSibling := parent.Children[index+1]
		leaf.Key = append(leaf.Key, rightSibling.Key...)
		leaf.Value = append(leaf.Value, rightSibling.Value...)
		leaf.Next = rightSibling.Next
		t.deleteFromParent(parent, index, index+1)
	}
	// 处理父节点删完的情况
	if parent.Parent == nil {
		if len(parent.Children) == 1 {
			t.root = parent.Children[0]
			t.root.Parent = nil
		}
	}
}

func (t *Tree) deleteFromParent(parent *Node, indexLeft int, indexRight int) {

	parent.Key = append(parent.Key[:indexLeft], parent.Key[indexLeft+1:]...)
	parent.Children = append(parent.Children[:indexRight], parent.Children[indexRight+1:]...)

	if len(parent.Key) < t.LeastLen {
		//如果父节点的key小于最小长度，需要平衡父节点，也就是非叶结点
		t.balanceNonLeafNode(parent)
	}

}
func (t *Tree) balanceNonLeafNode(node *Node) {

	if node.Parent == nil {
		return
	}
	parent := node.Parent
	index := t.getNodeIndex(parent, node)
	// 借左右子节点
	if index > 0 {
		leftSibling := parent.Children[index-1]
		if len(leftSibling.Key) > t.LeastLen {
			borrowKey := leftSibling.Key[len(leftSibling.Key)-1]
			borrowChild := leftSibling.Children[len(leftSibling.Children)-1]

			//leftSibling.Key = append(leftSibling.Key, parent.Key[index-1])
			node.Key = t.insertSlice(node.Key, 0, borrowKey)
			node.Children = t.insertNodeSlice(node.Children, 0, borrowChild)
			borrowChild.Parent = node
			parent.Key[index-1] = leftSibling.Key[len(leftSibling.Key)-2]

			leftSibling.Key = leftSibling.Key[:len(leftSibling.Key)-1]
			leftSibling.Children = leftSibling.Children[:len(leftSibling.Key)-1]
			return
		}
	}
	if index < len(parent.Key)-1 {
		rightSibling := parent.Children[index+1]
		if len(rightSibling.Key) > t.LeastLen {
			borrowKey := rightSibling.Key[0]
			borrowChild := rightSibling.Children[0]

			//node.Key = append(node.Key, parent.Key[index])
			node.Key = append(node.Key, borrowKey)
			node.Children = append(node.Children, borrowChild)
			borrowChild.Parent = node
			parent.Key[index] = borrowKey

			rightSibling.Key = rightSibling.Key[1:]
			rightSibling.Children = rightSibling.Children[1:]
			return
		}
	}
	// 没借成功，合并
	if index > 0 {
		leftSibling := parent.Children[index-1]
		leftSibling.Key = append(leftSibling.Key, node.Key...)
		leftSibling.Children = append(leftSibling.Children, node.Children...)
		for _, child := range node.Children {
			child.Parent = leftSibling
		}
		t.deleteFromParent(parent, index-1, index)
	} else {
		rightSibling := parent.Children[index+1]
		node.Key = append(node.Key, rightSibling.Key...)
		node.Children = append(node.Children, rightSibling.Children...)
		for _, child := range rightSibling.Children {
			child.Parent = node
		}
		t.deleteFromParent(parent, index, index+1)
	}
	if parent.Parent == nil {
		if len(parent.Children) == 1 {
			t.root = parent.Children[0]
			t.root.Parent = nil
		}
	}
}

// 查找操作
func (t *Tree) Search(key string) (*DataPair, int, bool) {
	if t.root == nil {
		return nil, -1, false
	}
	intKey := utils.ToHash(key)
	current := t.root
	for !current.IsLeaf {
		index := t.findIndex(current.Key, intKey)
		current = current.Children[index]
	}
	index := t.findIndex(current.Key, intKey)
	if index >= len(current.Value) {
		return nil, -1, false
	}
	d, i := Filter(current.Value[index], key)

	if d != nil {
		return d, i, true
	}
	return nil, -1, false
}

// 如果节点有多个数据，也就是hash冲突，遍历找到需要的originKey
func Filter(data []*DataPair, key string) (*DataPair, int) {
	for i, v := range data {
		if v.OriginKey == key {
			return v, i
		}
	}
	return nil, -1
}

// 打印树，通过将每个节点添加到queue队列最后打印
func (t *Tree) Print() {

	if t.root == nil {
		return
	}
	queue := []*Node{t.root}
	for len(queue) > 0 {
		levelSize := len(queue)
		for i := 0; i < levelSize; i++ {
			current := queue[0]

			queue = queue[1:]
			for _, k := range current.Key {
				fmt.Print(k, " ")
			}
			if i < levelSize-1 {
				fmt.Print(" ][")
			}
			if !current.IsLeaf {
				queue = append(queue, current.Children...)
			}
		}
		fmt.Println()
	}
}

// 插入操作
func (t *Tree) Insert(key int, value interface{}, originKey string) { //需要把最大值的key修改
	if t.root == nil {
		t.root = &Node{
			Key:      []int{key},
			Value:    [][]*DataPair{{&DataPair{OriginKey: originKey, Value: value, V: time.Now().UnixNano(), Update: true, CreatedAt: time.Now()}}},
			Children: []*Node{},
			IsLeaf:   true,
			Next:     nil,
		}
		return
	}
	leaf := t.findLeafNode(key)
	index := t.findIndex(leaf.Key, key)
	_, i, exist := t.Search(originKey)

	if index < len(leaf.Key) && leaf.Key[index] == key {
		if !exist {
			leaf.Value[index] = append(leaf.Value[index], &DataPair{OriginKey: originKey, Value: value, V: time.Now().UnixNano(), Update: true, CreatedAt: time.Now()})
			return
		} else {
			// 更新
			leaf.Value[index][i] = &DataPair{OriginKey: originKey, Value: value, V: time.Now().UnixNano(), Update: true, CreatedAt: leaf.Value[index][i].CreatedAt}
			return
		}
	}
	// 修改插入某个节点的key最大值的情况
	if key > leaf.Key[len(leaf.Key)-1] {
		current := leaf
		for current.Parent != nil {
			currentIndex := t.getNodeIndex(current.Parent, current)
			current = current.Parent
			current.Key[currentIndex] = key
		}
	}
	//普通插入操作
	leaf.Key = t.insertSlice(leaf.Key, index, key)
	leaf.Value = t.insertValueSlice(leaf.Value, index, []*DataPair{&DataPair{OriginKey: originKey, Value: value, V: time.Now().UnixNano(), Update: true, CreatedAt: time.Now()}})

	if len(leaf.Key) > t.MaxLen {
		//超过最大长度，需要分裂节点
		t.splitLeafNode(leaf)
	}

}

func (t *Tree) splitLeafNode(node *Node) {
	rangeIndex := (len(node.Key) + 1) / 2
	newNode := &Node{
		Key:      make([]int, len(node.Key[rangeIndex:])),
		Value:    make([][]*DataPair, len(node.Key[rangeIndex:])),
		Children: []*Node{},
		IsLeaf:   true,
		Next:     node.Next,
		Parent:   node.Parent,
	}
	copy(newNode.Key, node.Key[rangeIndex:])
	copy(newNode.Value, node.Value[rangeIndex:])
	node.Next = newNode
	node.Key = node.Key[:rangeIndex]
	node.Value = node.Value[:rangeIndex]
	t.insertToParent(node, newNode)

}

// 分裂的节点插入父节点
func (t *Tree) insertToParent(left *Node, right *Node) {
	parent := left.Parent
	if parent == nil {
		newRoot := &Node{
			Key:      []int{left.Key[len(left.Key)-1], right.Key[len(right.Key)-1]},
			Children: []*Node{left, right},
			IsLeaf:   false,
		}
		left.Parent = newRoot
		right.Parent = newRoot
		t.root = newRoot
		return
	}
	insertIndex := t.findIndex(parent.Key, left.Key[len(left.Key)-1])
	parent.Key = t.insertSlice(parent.Key, insertIndex, left.Key[len(left.Key)-1])
	parent.Children = t.insertNodeSlice(parent.Children, insertIndex+1, right)
	//父节点超过最大长度，分裂内部节点
	if len(parent.Key) > t.MaxLen {
		t.splitNonLeafNode(parent)
	}
}
func (t *Tree) splitNonLeafNode(node *Node) {
	rangeIndex := (len(node.Key) + 1) / 2
	newNode := &Node{
		Key:      make([]int, len(node.Key[rangeIndex:])),
		Children: make([]*Node, len(node.Children[rangeIndex:])),
		IsLeaf:   false,
		Parent:   node.Parent,
		Next:     node.Next,
	}
	copy(newNode.Key, node.Key[rangeIndex:])
	copy(newNode.Children, node.Children[rangeIndex:])
	for _, child := range newNode.Children {
		child.Parent = newNode
	}
	node.Key = node.Key[:rangeIndex]
	node.Children = node.Children[:rangeIndex]
	node.Next = newNode
	t.insertToParent(node, newNode)
}

// 几个辅助函数，辅助插入和查找index操作
func (t *Tree) insertSlice(slice []int, index int, value int) []int {
	slice = append(slice, 0)
	copy(slice[index+1:], slice[index:])
	slice[index] = value
	return slice
}
func (t *Tree) insertValueSlice(slice [][]*DataPair, index int, value []*DataPair) [][]*DataPair {
	slice = append(slice, nil)
	copy(slice[index+1:], slice[index:])
	slice[index] = value
	return slice
}

func (t *Tree) insertNodeSlice(slice []*Node, index int, value *Node) []*Node {
	slice = append(slice, nil)
	copy(slice[index+1:], slice[index:])
	slice[index] = value
	return slice
}
func (t *Tree) getNodeIndex(parent *Node, node *Node) int {
	for i, child := range parent.Children {
		if child == node {
			return i
		}
	}
	return -1
}

func (t *Tree) findLeafNode(key int) *Node {
	current := t.root
	for !current.IsLeaf {
		index := t.findIndex(current.Key, key)
		if index >= len(current.Children) {
			index = len(current.Children) - 1
		}
		current = current.Children[index]

	}
	return current
}

func (t *Tree) findIndex(keys []int, key int) int {
	index := 0
	for index < len(keys) && keys[index] < key {
		index++
	}
	return index
}
