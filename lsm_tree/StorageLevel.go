package lsm_tree

import (
	"dmds_lab2/shared"
	"dmds_lab2/ss_table"
	"errors"
	"math"
	"os"
	"path"
	"strconv"
)

// StorageLevel represents a storage level in the LSM Tree, it contains a list of SSTables
type StorageLevel struct {
	index            uint64                       // Index of the storage level
	count            uint64                       // Number of key-value pairs in the storage level
	ssTables         map[string]*ss_table.SSTable // List of SSTables in the storage level
	ssTablesOrdered  []string
	ssTablesToRemove []string
}

func (L *StorageLevel) addSSTable(sst *ss_table.SSTable, filename string) {
	L.ssTables[filename] = sst
	L.ssTablesOrdered = append(L.ssTablesOrdered, filename)
	L.count += uint64(len(sst.GetData())) / shared.BlockSize
}

func (L *StorageLevel) removeSSTable(filename string) error {
	ssTable, ok := L.ssTables[filename]
	if !ok {
		return errors.New("SSTable not found")
	}

	if err := ssTable.Close(); err != nil {
		return err
	}

	if err := ssTable.Delete(); err != nil {
		return err
	}

	L.count -= uint64(len(L.ssTables[filename].GetData())) / shared.BlockSize

	delete(L.ssTables, filename)

	// Could be optimized
	for i, s := range L.ssTablesOrdered {
		if s == filename {
			L.ssTablesOrdered = append(L.ssTablesOrdered[:i], L.ssTablesOrdered[i+1:]...)
			break
		}
	}

	return nil
}

// GetPath returns the path of the storage level where the SSTables are stored
func (L *StorageLevel) GetPath() string {
	return path.Join(shared.SSTablesRootDirectory, strconv.Itoa(int(L.GetIndex())))
}

// GetCount returns the number of key-value pairs in the storage level
func (L *StorageLevel) GetCount() uint64 {
	return L.count
}

// GetIndex returns the index of the storage level
func (L *StorageLevel) GetIndex() uint64 {
	return L.index
}

// GetMaxNumberOfSSTables returns the maximum number of SSTables that can be stored in the storage level
func (L *StorageLevel) GetMaxNumberOfSSTables() uint64 {
	return L.GetMaxCount() / shared.FirstLevelMaxSize
}

// GetMaxCount returns the maximum number of key-value pairs that can be stored in the storage level
// according to the formula: FirstLevelMaxSize * GrowthFactor^(index+1)
func (L *StorageLevel) GetMaxCount() uint64 {
	a := shared.FirstLevelMaxSize
	b := math.Pow(float64(shared.GrowthFactor), float64(L.index))
	return a * uint64(b)
}

// IsFull returns true if the storage level is full
// Returns true if the number of key-value pairs in the storage level is greater than or equal to
// the maximum number of key-value pairs that can be stored in the storage level
func (L *StorageLevel) IsFull() bool {
	return L.GetCount() >= L.GetMaxCount()
}

// Add adds a new SSTable to the storage level
// This will write the SSTable to disk with the following format: <minKey>_<maxKey>_<randomString>.sst
func (L *StorageLevel) Add(sstInt interface{}) error {
	sst, ok := sstInt.(*ss_table.SSTable)
	if !ok {
		return errors.New("sstInt is not of type *ss_table.SSTable")
	}

	// Write the sst to disk
	meta, err := sst.GetMetadata()
	if err != nil {
		return err
	}

	fileName := strconv.FormatUint(meta.GetMinKey(), 10) + "_" + strconv.FormatUint(meta.GetMaxKey(), 10) + "_" + shared.RandomString(32) + shared.SSTableExtension
	sst.SetPath(path.Join(L.GetPath(), fileName))

	L.addSSTable(sst, fileName)

	if err := sst.Create(); err != nil {
		return err
	}

	if err := sst.Write(); err != nil {
		return err
	}

	return nil
}

// Remove removes an SSTable from the storage level
// This will close the SSTable and delete the file from disk
func (L *StorageLevel) RemoveFlushedComponent() error {
	for _, filename := range L.ssTablesToRemove {
		if err := L.removeSSTable(filename); err != nil {
			return err
		}
	}

	L.ssTablesToRemove = make([]string, 0)

	return nil
}

// AsArray returns the storage level as a byte array
// This will concatenate all the SSTables in the storage level calling the GetData() method on each SSTable
func (L *StorageLevel) AsArray() []byte {
	buf := make([]byte, 0)
	for _, ssTable := range L.ssTables {
		data := ssTable.GetData()
		buf = append(buf, data...)
	}
	return buf
}

// Close closes all the SSTables in the storage level
func (L *StorageLevel) Close() error {
	for _, ssTable := range L.ssTables {
		err := ssTable.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// Load loads all the SSTables in the storage level
// Since the SSTables files are prefixed with the minKey and maxKey, we can load them in order of the file names (TODO: make this more reliable).
// This will load the metadata (minKey, maxKey), data and create the index (SkipList for each SSTable).
// The SSTables files are closed after loading because we only need the metadata and index in memory.
func (L *StorageLevel) Load() error {
	// Scan the directory for SSTables
	files, err := os.ReadDir(L.GetPath())
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		ssTable := ss_table.NewSSTable()
		ssTable.SetPath(path.Join(L.GetPath(), file.Name()))
		if err := ssTable.Open(); err != nil {
			return err
		}

		if err = ssTable.LoadMetadata(); err != nil {
			return err
		}

		if err = ssTable.LoadDataToMemory(); err != nil {
			return err
		}

		if err = ssTable.CreateIndex(); err != nil {
			return err
		}

		if err = ssTable.CreateBloomFilter(shared.HashFunctions); err != nil {
			return err
		}

		if err = ssTable.Close(); err != nil {
			return err
		}

		L.addSSTable(ssTable, file.Name())
	}

	return nil
}

// Get returns the value of the key
// This will iterate through the SSTables in the storage level and call the Get() method on each if the key is within the range of the SSTable
func (L *StorageLevel) Get(key shared.KeyType) (*shared.ValueType, string, error) {
	for _, ssTable := range L.ssTables {
		metadata, err := ssTable.GetMetadata()
		if err != nil {
			return nil, "", err
		}
		if metadata.GetMinKey() <= key && metadata.GetMaxKey() >= key {
			value, err := ssTable.Get(key)
			return value, ssTable.GetPath(), err
		}
	}
	return nil, "", shared.KeyNotFoundError
}

// FlushFirstComponent flushes the first component of the storage level
// This will return the data of the first SSTable in the storage level and the minKey and maxKey of the SSTable
func (L *StorageLevel) FlushFirstComponent() ([]byte, shared.KeyType, shared.KeyType, error) {
	ssTable := L.ssTables[L.ssTablesOrdered[0]]
	meta, err := ssTable.GetMetadata()
	if err != nil {
		return nil, 0, 0, err
	}
	L.ssTablesToRemove = append(L.ssTablesToRemove, L.ssTablesOrdered[0])
	return ssTable.GetData(), meta.GetMinKey(), meta.GetMaxKey(), nil
}

// InsertFlushedData inserts the flushed data to the storage level
// The data, minKey and maxKey parameter is coming from a higher level (memory or storage) that has been flushed
// 1. Find the SSTables in the current storage level that overlap with the flushed data
// 2. Merge the flushed data with the SSTables that overlap with the flushed data according to the minKey and maxKey using K-way merge algorithm
// 3. Create new SSTables with the merged data
// 4. Remove the old SSTables in the current storage level that has been merged
// 5. Add N (Replace the old SSTables) new SSTables to the current storage level, where N slices of the merged data are created to fit the component size (FirstLevelMaxSize)
// Since we are using the Tiering Policy, the higher level loop will check if the storage level is full and flush the first component to the next level etc.
func (L *StorageLevel) InsertFlushedData(data []byte, minKey shared.KeyType, maxKey shared.KeyType) error {
	// Take 2 first parts and merge them
	dataToMerge := make([][]byte, 0)
	dataToMerge = append(dataToMerge, data)

	// Find the SSTables that overlap with the flushed data
	for _, ssTableFileName := range L.ssTablesOrdered {
		ssTable := L.ssTables[ssTableFileName]

		// Check the overlap
		metadata, err := ssTable.GetMetadata()
		if err != nil {
			return err
		}

		// If there is an overlap, merge the data
		if minKey <= metadata.GetMaxKey() && metadata.GetMinKey() <= maxKey {
			L.ssTablesToRemove = append(L.ssTablesToRemove, ssTableFileName)
			dataToMerge = append(dataToMerge, ssTable.GetData())
		}
	}

	// Remove duplicates
	dataToMerge = MergeDuplicatedKeys(dataToMerge)

	// Remove the tombstones from the data
	dataWithoutTombstones, err := RemoveTombstones(dataToMerge)
	if err != nil {
		return err
	}

	// Merge the data
	data, err = MergeSortedArray(dataWithoutTombstones)
	if err != nil {
		return err
	}

	// Create new SSTables with the merged data, since we are using the Tiering Policy, we will create N new SSTables
	// which fits the component size (FirstLevelMaxSize).
	// The higher level loop will check is the storage level is full and flush the first component to the next level etc.
	for startIndex := uint64(0); startIndex < uint64(len(data)); startIndex += shared.FirstLevelMaxSize * shared.BlockSize {
		endIndex := min(startIndex+shared.FirstLevelMaxSize*shared.BlockSize, uint64(len(data)))
		currentMinKey, err := shared.ByteToKey(data[startIndex : startIndex+shared.KeySize])
		if err != nil {
			return err
		}
		currentMaxKey, err := shared.ByteToKey(data[endIndex-shared.BlockSize : endIndex-shared.BlockSize+shared.KeySize])
		if err != nil {
			return err
		}

		meta := ss_table.NewMetadata(currentMinKey, currentMaxKey)
		ssTable := ss_table.NewSSTable()
		ssTable.SetData(data[startIndex:endIndex])
		ssTable.SetMetadata(meta)
		if err := ssTable.CreateIndex(); err != nil {
			return err
		}
		if err := ssTable.CreateBloomFilter(shared.HashFunctions); err != nil {
			return err
		}
		if err := L.Add(ssTable); err != nil {
			return err
		}
	}

	return nil
}

// InitializeStorage initializes the storage level by creating the directory where the SSTables are stored
func (L *StorageLevel) InitializeStorage() error {
	if err := os.MkdirAll(L.GetPath(), 0755); err != nil {
		return err
	}
	return nil
}

func NewStorageLevel(index uint64) *StorageLevel {
	return &StorageLevel{
		index:           index,
		count:           0,
		ssTables:        make(map[string]*ss_table.SSTable),
		ssTablesOrdered: make([]string, 0),
	}
}
