package lsm_tree

import (
	"dmds_lab2/shared"
	"dmds_lab2/skip_list"
	"errors"
	"os"
	"path"
	"strconv"
)

// MemoryLevel represents a memory level in the LSM Tree, it contains a SkipList
type MemoryLevel struct {
	index            uint64              // Index of the memory level should always be 0
	skipList         *skip_list.SkipList // SkipList in the memory level
	osFile           *os.File            // File to store the key-value pairs
	currentFileName  string              // Name of the file that is currently being used to store the key-value pairs
	fileNameToDelete string              // Name of the file that is going to be deleted
}

// GetPath returns the path of the storage level where the logs are stored
func (L *MemoryLevel) GetPath() string {
	return path.Join(shared.SSTablesRootDirectory, strconv.FormatUint(L.index, 10))
}

// GetCount returns the number of key-value pairs in the level (SkipList)
func (L *MemoryLevel) GetCount() uint64 {
	if L.skipList == nil {
		return 0
	}
	return L.skipList.GetCount()
}

// GetIndex returns the index of the level, should always be 0
func (L *MemoryLevel) GetIndex() uint64 {
	return L.index
}

// GetMaxCount returns the maximum number of key-value pairs that can be stored in the level
func (L *MemoryLevel) GetMaxCount() uint64 {
	return shared.FirstLevelMaxSize
}

// Add adds a new SkipList to the level
func (L *MemoryLevel) Add(slInt interface{}) error {
	sl, ok := slInt.(*skip_list.SkipList)
	if !ok {
		return errors.New("slInt is not of type *skip_list.SkipList")
	}

	L.skipList = sl

	if err := L.InitializeStorage(); err != nil {
		return err
	}

	return nil
}

// RemoveFlushedComponent removes the old SkipList from the level
func (L *MemoryLevel) RemoveFlushedComponent() error {
	if L.fileNameToDelete == "" {
		return errors.New("no file to delete")
	}

	if err := os.Remove(path.Join(L.GetPath(), L.fileNameToDelete) + shared.SkipListExtension); err != nil {
		return err
	}

	L.fileNameToDelete = ""

	return nil
}

// AsArray returns the key-value pairs in the level as a byte slice
// This will concatenate all the key-value pairs in the SkipList calling the KeyValueToByte() method on each key-value pair
func (L *MemoryLevel) AsArray() []byte {
	buf := make([]byte, 0)
	current := L.skipList.GetHead()
	for current != nil {
		buf = append(buf, shared.KeyValueToByte(current.GetKey(), current.GetValue())...)
		current = current.GetNext()[0]
	}

	return buf
}

// Get returns the value of the key
func (L *MemoryLevel) Get(key shared.KeyType) (*shared.ValueType, string, error) {
	value, err := L.skipList.Get(key)
	return value, path.Join(L.GetPath(), L.currentFileName), err
}

// IsFull returns true if the level is full
func (L *MemoryLevel) IsFull() bool {
	return L.GetCount() >= L.GetMaxCount()
}

// Close closes the log file
func (L *MemoryLevel) Close() error {
	if L.osFile != nil {
		if err := L.osFile.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Load loads the key-value pairs from the log file into the SkipList
// If the log file does not exist, it creates a new one.
// If the log file exists, it reads the key-value pairs from the file and inserts them into the SkipList that is stored in the MemoryLevel
func (L *MemoryLevel) Load() error {
	// Scan the directory for the cache file
	files, err := os.ReadDir(L.GetPath())
	if err != nil {
		return err
	}

	// If there is no cache file, create one
	if len(files) == 0 {
		L.currentFileName = shared.RandomString(32) // To change to more reliable name
		osFile, err := os.Create(path.Join(L.GetPath(), L.currentFileName) + shared.SkipListExtension)
		if err != nil {
			return err
		}
		L.osFile = osFile
		L.skipList = skip_list.NewSkipList()
		return nil
	}

	// Load from the cache file
	osFile, err := os.Open(path.Join(L.GetPath(), files[0].Name()))
	if err != nil {
		return err
	}
	L.osFile = osFile

	// Read the cache file
	buf := make([]byte, shared.BlockSize)
	for {
		n, err := L.osFile.Read(buf)
		if err != nil {
			break
		}

		for i := uint64(0); i < uint64(n); i += shared.BlockSize {
			key, value, err := shared.ByteToKeyValue(buf[i : i+shared.BlockSize])
			if err != nil {
				return err
			}
			err = L.skipList.Insert(key, value)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// InitializeStorage creates the directory for the memory level
func (L *MemoryLevel) InitializeStorage() error {
	if err := os.MkdirAll(path.Join(L.GetPath()), 0755); err != nil {
		return err
	}

	return nil
}

// Insert inserts the key-value pair into the SkipList and the log file
func (L *MemoryLevel) Insert(key shared.KeyType, value shared.ValueType) error {
	err := L.skipList.Insert(key, value)
	if err != nil {
		return err
	}

	// Write the key-value pair to the cache file
	_, err = L.osFile.Write(shared.KeyValueToByte(key, value))
	if err != nil {
		return err
	}

	if L.IsFull() {
		return shared.LevelFullError
	}

	return nil
}

// FlushFirstComponent flushes the first component of the level
// 1. It closes the log file
// 2. It creates a new logs file for the new SkipList that will be created
// 3. It returns the key-value pairs in the old SkipList, the minKey and maxKey of the old SkipList
func (L *MemoryLevel) FlushFirstComponent() ([]byte, uint64, uint64, error) {
	data := L.AsArray()
	oldSkipList := L.skipList
	L.fileNameToDelete = L.currentFileName

	// Close the cache file
	if err := L.osFile.Close(); err != nil {
		return nil, 0, 0, err
	}

	// Create the cache file
	L.currentFileName = shared.RandomString(32) // To change to more reliable name
	osFile, err := os.Create(path.Join(L.GetPath(), L.currentFileName) + shared.SkipListExtension)
	if err != nil {
		return nil, 0, 0, err
	}
	L.osFile = osFile

	L.skipList = skip_list.NewSkipList()

	return data, oldSkipList.GetHead().GetKey(), oldSkipList.GetTail().GetKey(), nil
}

func NewMemoryLevel(index uint64) *MemoryLevel {
	return &MemoryLevel{
		index:    index,
		skipList: skip_list.NewSkipList(),
	}
}
