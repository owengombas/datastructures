package lsm_tree

import (
	"dmds_lab2/shared"
	"errors"
)

type LSMTree struct {
	levels        []Level
	rootDirectory string
	maxLevel      uint64
}

// Insert inserts the key-value pair into the LSM Tree, it first inserts the key-value pair into the SkipList
// and once the SkipList reaches the limit it compacts the level and moves the data to the next level.
func (L *LSMTree) Insert(key shared.KeyType, value shared.ValueType) error {
	err := L.levels[0].(*MemoryLevel).Insert(key, value)
	if err == nil {
		return nil
	}

	if !errors.Is(err, shared.LevelFullError) {
		return err
	}

	for levelIndex := 0; levelIndex < int(L.maxLevel-1); levelIndex++ {
		level := L.levels[levelIndex]
		if !level.IsFull() {
			continue
		}

		nextLevel := L.levels[levelIndex+1].(*StorageLevel)
		flushedData, minKey, mayKey, err := level.FlushFirstComponent()
		if err != nil {
			return err
		}
		if err = nextLevel.InsertFlushedData(flushedData, minKey, mayKey); err != nil {
			return err
		}
		if err = level.RemoveFlushedComponent(); err != nil {
			return err
		}
		if err = nextLevel.RemoveFlushedComponent(); err != nil {
			return err
		}
	}

	return nil
}

// Get returns the value of the key, it iterates through the levels and calls the Get() method on each level.
func (L *LSMTree) Get(key shared.KeyType) (*shared.ValueType, Level, string, error) {
	for _, level := range L.levels {
		value, source, err := level.Get(key)
		if err == nil {
			if shared.IsTombstone(*value) {
				return nil, nil, "", shared.KeyTombstonedError
			}
			return value, level, source, nil
		}
	}
	return nil, nil, "", shared.KeyNotFoundError
}

// Delete inserts a tombstone value for the key, the key will be marked as deleted.
func (L *LSMTree) Delete(key shared.KeyType) error {
	return L.Insert(key, shared.TombstoneValue)
}

// Close closes the LSM Tree, it closes all the levels (either MemoryLevel or StorageLevel)
func (L *LSMTree) Close() error {
	for _, level := range L.levels {
		if err := level.Close(); err != nil {
			return err
		}
	}

	return nil
}

// NewLSMTree creates a new LSM Tree, it initializes the SkipList
// It also creates the cache file if it does not exist.
func NewLSMTree(rootDirectory string, maxLevel uint64) (*LSMTree, error) {
	lsmTree := &LSMTree{
		rootDirectory: rootDirectory,
		levels:        make([]Level, maxLevel),
		maxLevel:      maxLevel,
	}

	lsmTree.levels[0] = NewMemoryLevel(0)
	for i := uint64(1); i < maxLevel; i++ {
		lsmTree.levels[i] = NewStorageLevel(i)
	}

	for _, level := range lsmTree.levels {
		if err := level.InitializeStorage(); err != nil {
			return nil, err
		}
		if err := level.Load(); err != nil {
			return nil, err
		}
	}

	return lsmTree, nil
}
