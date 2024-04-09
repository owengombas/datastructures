package lsm_tree

import "dmds_lab2/shared"

// Level represents a level in the LSM Tree
type Level interface {
	GetPath() string
	GetCount() uint64
	GetIndex() uint64
	GetMaxCount() uint64
	Add(structure interface{}) error
	IsFull() bool
	AsArray() []byte
	Close() error
	Load() error
	InitializeStorage() error
	Get(key shared.KeyType) (*shared.ValueType, string, error)
	FlushFirstComponent() ([]byte, shared.KeyType, shared.KeyType, error)
	RemoveFlushedComponent() error
}
