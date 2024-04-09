package b_plus_tree

import (
	"dmds_lab2/shared"
	"sort"
)

type LeafNode struct {
	keys     []shared.KeyType
	values   []*shared.ValueType
	next     *LeafNode
	count    uint64
	capacity uint64
}

func NewLeafNode(capacity uint64) *LeafNode {
	return &LeafNode{
		keys:     make([]shared.KeyType, capacity+1),
		values:   make([]*shared.ValueType, capacity+1),
		capacity: capacity,
	}
}

func (ln *LeafNode) IsFull() bool {
	return ln.count >= ln.capacity
}

func (ln *LeafNode) GetKeys() []shared.KeyType {
	return ln.keys[:len(ln.keys)-1]
}

func (ln *LeafNode) GetNext() *LeafNode {
	return ln.next
}

func (ln *LeafNode) GetValueAtIndex(index uint64) *shared.ValueType {
	if index >= uint64(len(ln.values)) {
		return nil
	}
	return ln.values[index]
}

func (ln *LeafNode) Scan(key shared.KeyType) (uint64, error) {
	idx := sort.Search(len(ln.keys), func(i int) bool {
		if ln.keys[i] == 0 {
			return true
		}
		return ln.keys[i] >= key
	})

	if idx == -1 {
		return 0, shared.KeyNotFoundError
	}

	return uint64(idx), nil
}

func (ln *LeafNode) Split() (*LeafNode, *LeafNode, shared.KeyType) {
	midIdx := ln.capacity / 2
	midKey := ln.keys[midIdx]

	newLeafNode := NewLeafNode(ln.capacity)
	copy(newLeafNode.keys, ln.keys[midIdx+1:])
	copy(newLeafNode.values, ln.values[midIdx+1:])
	newLeafNode.count = ln.count - midIdx

	for i := midIdx + 1; i < ln.capacity+1; i++ {
		ln.keys[i] = 0
		ln.values[i] = nil
	}
	ln.count = midIdx + 1

	newLeafNode.next = ln.next
	ln.next = newLeafNode

	return ln, newLeafNode, midKey
}

func (ln *LeafNode) MakeSpaceAtIndex(index uint64) {
	ln.keys = append(ln.keys[:index+1], ln.keys[index:len(ln.keys)-1]...)
	ln.values = append(ln.values[:index+1], ln.values[index:len(ln.values)-1]...)
}

func (ln *LeafNode) Insert(key shared.KeyType) (Node, Node, shared.KeyType, error) {
	idx, _ := ln.Scan(key)
	ln.MakeSpaceAtIndex(idx)

	if ln.IsFull() {
		ln.keys[idx] = key
		l1, l2, _ := ln.Split()
		return l1, l2, l2.keys[0], nil
	}

	ln.keys[idx] = key
	ln.count++

	return ln, nil, shared.KeyType(0), nil
}
