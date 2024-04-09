package b_plus_tree

import (
	"dmds_lab2/shared"
)

type BPlusTree struct {
	root  *InteriorNode
	count uint64
}

func NewBPlusTree(capacity uint64) *BPlusTree {
	root := NewInteriorNode(capacity)
	root.isRoot = true
	leaf1 := NewLeafNode(capacity)
	leaf2 := NewLeafNode(capacity)
	leaf1.next = leaf2
	root.next[0] = leaf1
	root.next[1] = leaf2
	return &BPlusTree{
		root: root,
	}
}

func (t *BPlusTree) Get(key shared.KeyType) (*LeafNode, uint64, []*InteriorNode, error) {
	var current Node = t.root
	var currentLeaf *LeafNode
	path := make([]*InteriorNode, 0)

	for currentLeaf == nil {
		idx, err := current.Scan(key)
		if err != nil {
			return nil, idx, path, err
		}

		currentInterior, isInteriorNode := current.(*InteriorNode)
		path = append(path, currentInterior)
		if isInteriorNode {
			nextNode := currentInterior.GetNodeAtIndex(idx)
			if nextNode == nil {
				return nil, idx, path, shared.KeyNotFoundError
			}
			current = nextNode
		}

		currentLeafCheck, isLeafNode := current.(*LeafNode)
		if isLeafNode {
			currentLeaf = currentLeafCheck
		}
	}

	idx, err := currentLeaf.Scan(key)

	return currentLeaf, idx, path, err
}

func (t *BPlusTree) Insert(key shared.KeyType) (*LeafNode, []*InteriorNode, error) {
	if t.count <= 0 {
		_, _, _, err := t.root.Insert(key, t.root.next[0], t.root.next[1])
		t.count++
		return nil, []*InteriorNode{t.root}, err
	}

	leaf, _, path, _ := t.Get(key)
	s1, s2, midKey, err := leaf.Insert(key)
	if err != nil {
		return nil, nil, err
	}

	if s2 == nil {
		t.count++
		return leaf, path, nil
	}

	for i := len(path) - 1; i >= 0; i-- {
		in := path[i]

		var s2Next *InteriorNode
		var s1Next *InteriorNode
		s1Next, s2Next, midKey, err = in.Insert(midKey, s1, s2)
		if err != nil {
			return nil, nil, err
		}

		if i == 0 {
			t.root = s1Next
		}

		if s2Next == nil {
			break
		}
		s1 = s1Next
		s2 = s2Next
	}

	t.count++
	return leaf, path, nil
}
