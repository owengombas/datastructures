package b_plus_tree

import (
	"dmds_lab2/shared"
	"errors"
)
import "sort"

type InteriorNode struct {
	keys   []shared.KeyType
	next   []Node
	count  int
	isRoot bool
}

func NewInteriorNode(capacity uint64) *InteriorNode {
	return &InteriorNode{
		keys: make([]shared.KeyType, capacity),
		next: make([]Node, capacity+1),
	}
}

func (in *InteriorNode) IsFull() bool {
	return in.count >= cap(in.keys)
}

func (in *InteriorNode) GetNodeAtIndex(index uint64) Node {
	if index > uint64(len(in.next)) {
		return nil
	}
	return in.next[index]
}

func (in *InteriorNode) Scan(key shared.KeyType) (uint64, error) {
	idx := sort.Search(len(in.keys), func(i int) bool {
		if in.keys[i] == 0 {
			return true
		}
		return in.keys[i] > key
	})

	if idx == -1 {
		return uint64(cap(in.keys)), nil
	}

	return uint64(idx), nil
}

func (in *InteriorNode) MakeSpaceAtIndex(index uint64) error {
	if index < 0 || index > uint64(len(in.keys)) {
		return errors.New("index out of range")
	}
	if in.count+1 > cap(in.keys) {
		return errors.New("out of space")
	}
	in.keys = append(in.keys[:index+1], in.keys[index:len(in.keys)-1]...)
	in.next = append(in.next[:index+1], in.next[index:len(in.next)-1]...)
	return nil
}

func (in *InteriorNode) Insert(key shared.KeyType, node1 Node, node2 Node) (*InteriorNode, *InteriorNode, shared.KeyType, error) {
	idx, _ := in.Scan(key)

	if err := in.MakeSpaceAtIndex(idx); err != nil {
		return nil, nil, 0, nil
	}

	in.keys[idx] = key
	in.next[idx] = node1
	in.next[idx+1] = node2
	in.count++

	if in.IsFull() {
		s1, s2, midKey := in.Split()

		if in.isRoot {
			in.isRoot = false
			newRoot := NewInteriorNode(uint64(cap(in.keys)))
			newRoot.isRoot = true
			newRoot.count = 1
			newRoot.keys[0] = midKey
			newRoot.next[0] = s1
			newRoot.next[1] = s2

			return newRoot, nil, midKey, nil
		}

		return s1, s2, midKey, nil
	}

	return in, nil, 0, nil
}

func (in *InteriorNode) Split() (*InteriorNode, *InteriorNode, shared.KeyType) {
	midIdx := cap(in.keys) / 2
	midKey := in.keys[midIdx]

	newInteriorNode := NewInteriorNode(uint64(cap(in.keys)))
	copy(newInteriorNode.keys, in.keys[midIdx+1:])
	copy(newInteriorNode.next, in.next[midIdx+1:])
	newInteriorNode.count = len(in.keys) - midIdx - 1

	in.keys[midIdx] = 0
	for i := midIdx + 1; i < cap(in.keys); i++ {
		in.keys[i] = 0
		in.next[i] = nil
	}
	in.next[cap(in.next)-1] = nil
	in.count = midIdx

	return in, newInteriorNode, midKey
}
