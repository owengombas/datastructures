package skip_list

import (
	"dmds_lab2/shared"
)

type Node struct {
	key    shared.KeyType
	value  shared.ValueType
	next   []*Node
	height uint64
}

func (s *Node) GetHeight() uint64 {
	return s.height
}

func (s *Node) GetKey() shared.KeyType {
	return s.key
}

func (s *Node) GetValue() shared.ValueType {
	return s.value
}

func (s *Node) GetNext() []*Node {
	return s.next
}

// GetNodeLevel returns the height of the skip_list preserving the Node
// probability distribution.
func getNodeLevel(p float32, maxLevel uint64) uint64 {
	var level uint64 = 1
	for shared.RandomGenerator.Float32() < p && level < maxLevel {
		level++
	}
	return level
}

func newNode(height uint64, key shared.KeyType, value shared.ValueType) *Node {
	newSkipList := &Node{
		next:   make([]*Node, height),
		height: height,
		key:    key,
		value:  value,
	}
	return newSkipList
}
