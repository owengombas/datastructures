package skip_list

import (
	"dmds_lab2/shared"
)

// MaxLevel is the maximum level of the SkipList.
const maxLevel uint64 = 3 + 1

// P is the probability used to determine the level of the SkipList.
const p float32 = 0.5

type SkipList struct {
	head  *Node
	tail  *Node
	count uint64
}

func (s *SkipList) GetHead() *Node {
	return s.head.next[0]
}

func (s *SkipList) GetTail() *Node {
	return s.tail
}

func (s *SkipList) GetCount() uint64 {
	return s.count
}

// GetNode returns the node with the key.
func (s *SkipList) GetNode(key shared.KeyType) (*Node, error) {
	current := s.head
	for i := current.height; i > 0; i-- {
		idx := i - 1
		for current.next[idx] != nil && current.next[idx].key < key {
			current = current.next[idx]
		}
	}

	if current.next[0] != nil && current.next[0].key == key {
		return current.next[0], nil
	}

	return current, shared.KeyNotFoundError
}

// Get returns the value of the key.
func (s *SkipList) Get(key shared.KeyType) (*shared.ValueType, error) {
	node, err := s.GetNode(key)
	if err != nil {
		return nil, err
	}
	return &node.value, nil
}

// Insert inserts the key-value pair into the node.
func (s *SkipList) Insert(key shared.KeyType, value shared.ValueType) error {
	current := s.head
	newSkipListNode := newNode(getNodeLevel(p, maxLevel), key, value)

	for i := current.height; i > 0; i-- {
		idx := i - 1
		for current.next[idx] != nil && current.next[idx].key < key {
			current = current.next[idx]
		}
		if i <= newSkipListNode.height {
			newSkipListNode.next[idx] = current.next[idx]
			current.next[idx] = newSkipListNode
		}
	}

	if newSkipListNode.next[0] == nil {
		s.tail = newSkipListNode
	}

	s.count++
	return nil
}

// Update updates the value of the key.
func (s *SkipList) Update(key shared.KeyType, value shared.ValueType) error {
	refNode, err := s.GetNode(key)
	if err != nil {
		return err
	}
	refNode.value = value
	return nil
}

// Delete deletes the key from the node.
func (s *SkipList) Delete(key shared.KeyType) error {
	current := s.head
	for i := current.height; i > 0; i-- {
		idx := i - 1
		for current.next[idx] != nil && current.next[idx].key < key {
			current = current.next[idx]
		}
	}

	if current.next[0] != nil && current.next[0].key == key {
		current.next[0] = current.next[0].next[0]
		s.count--
		return nil
	}

	return shared.KeyNotFoundError
}

// NewSkipList returns a new SkipList.
func NewSkipList() *SkipList {
	head := newNode(maxLevel, 0, 0)
	return &SkipList{
		head:  head,
		tail:  head,
		count: 0,
	}
}
