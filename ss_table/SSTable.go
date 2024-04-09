package ss_table

import (
	"dmds_lab2/bloom_filter"
	"dmds_lab2/hash_function"
	"dmds_lab2/shared"
	"dmds_lab2/skip_list"
	"errors"
	"os"
)

// FileNotOpenError is the error returned when the file is not open.
var FileNotOpenError = errors.New("file not open")

var FileAlreadyOpenError = errors.New("file already open")

type SSTable struct {
	metadata     Metadata
	path         string
	osFile       *os.File
	array        []byte
	shallowIndex *skip_list.SkipList
	bloomFilter  *bloom_filter.BloomFilter
}

func (s *SSTable) GetPath() string {
	return s.path
}

func (s *SSTable) SetPath(path string) {
	s.path = path
}

func (s *SSTable) SetData(data []byte) {
	s.array = data
}

func (s *SSTable) GetMetadata() (*Metadata, error) {
	// If the metadata is empty, return nil and an error
	if s.metadata == (Metadata{}) {
		return nil, errors.New("metadata is empty")
	}
	return &s.metadata, nil
}

func (s *SSTable) SetMetadata(metadata Metadata) {
	s.metadata = metadata
}

func (s *SSTable) GetData() []byte {
	return s.array
}

// Create creates the file.
func (s *SSTable) Create() error {
	// Throw an error if the file is already open
	if s.osFile != nil {
		return FileAlreadyOpenError
	}

	// Throw an error if the file already exists
	exists, err := s.Exists()
	if err != nil {
		return err
	}
	if exists {
		return errors.New("file already exists")
	}

	osFile, err := os.Create(s.path) // Open the file with os.O_RDWR
	s.osFile = osFile
	if err != nil {
		return err
	}

	return nil
}

// Open opens the file in read-write mode.
func (s *SSTable) Open() error {
	if s.osFile != nil {
		return FileAlreadyOpenError
	}

	osFile, err := os.OpenFile(s.path, os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	s.osFile = osFile
	return nil
}

// Close closes the file.
func (s *SSTable) Close() error {
	if s.osFile == nil {
		return FileNotOpenError
	}

	return s.osFile.Close()
}

// Delete deletes the file.
func (s *SSTable) Delete() error {
	return os.Remove(s.path)
}

// Exists checks if the file exists.
func (s *SSTable) Exists() (bool, error) {
	_, err := os.Stat(s.path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// OpenOrCreate opens the file if it exists, otherwise creates it.
func (s *SSTable) OpenOrCreate() error {
	exists, err := s.Exists()
	if err != nil {
		return err
	}

	if exists {
		return s.Open()
	}
	return s.Create()
}

// GetFileByteSize returns the size of the file in bytes.
func (s *SSTable) GetFileByteSize() (uint64, error) {
	if s.osFile == nil {
		return 0, FileNotOpenError
	}

	fileInfo, err := s.osFile.Stat()
	if err != nil {
		return 0, err
	}

	return uint64(fileInfo.Size()), nil
}

// LoadMetadata loads the metadata from the file to the SSTable struct.
func (s *SSTable) LoadMetadata() error {
	if s.osFile == nil {
		return FileNotOpenError
	}

	_, err := s.osFile.Seek(0, 0)
	if err != nil {
		return err
	}

	metadata := [MetadataSize]byte{}
	_, err = s.osFile.Read(metadata[:])
	if err != nil {
		return err
	}

	s.metadata.FromByte(metadata)

	return nil
}

// readByte reads n bytes from the file starting from the start position, ignoring the metadata.
func (s *SSTable) readByte(start uint64, n uint64) ([]byte, error) {
	if s.osFile == nil {
		return nil, errors.New("file not open")
	}

	_, err := s.osFile.Seek(int64(MetadataSize+start), 0)
	if err != nil {
		return nil, err
	}

	data := make([]byte, n)
	_, err = s.osFile.Read(data)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// Get retrieves the value of the given key from the SSTable using binary search.
func (s *SSTable) Get(key uint64) (*shared.ValueType, error) {
	if s.shallowIndex == nil {
		return nil, errors.New("index not created")
	}

	if s.bloomFilter != nil {
		keyByte := shared.KeyToByte(key)
		exists := s.bloomFilter.Contains(keyByte)
		if !exists {
			return nil, shared.KeyNotFoundError
		}
	}

	return s.shallowIndex.Get(key)
}

func (s *SSTable) LoadDataToMemory() error {
	if s.osFile == nil {
		return FileNotOpenError
	}

	fileSize, err := s.GetFileByteSize()
	if err != nil {
		return err
	}

	data, err := s.readByte(0, fileSize-MetadataSize)
	if err != nil {
		return err
	}

	s.array = data
	return nil
}

// CreateIndex creates an index for the SSTable file, in this case, a SkipList.
// It loads the key-value pairs from the file and inserts them into the SkipList allowing for faster lookups.
func (s *SSTable) CreateIndex() error {
	if s.array == nil {
		return errors.New("data not loaded to memory")
	}

	skipList := skip_list.NewSkipList()

	for startIndex := uint64(0); startIndex < uint64(len(s.array)); startIndex += shared.BlockSize {
		endIndex := startIndex + shared.BlockSize

		key, value, err := shared.ByteToKeyValue(s.array[startIndex:endIndex])
		if err != nil {
			return err
		}

		err = skipList.Insert(key, value)
		if err != nil {
			return err
		}
	}

	s.shallowIndex = skipList
	return nil
}

func (s *SSTable) CreateBloomFilter(hashFunctions []hash_function.HashFunction) error {
	if s.array == nil {
		return errors.New("data not loaded to memory")
	}
	m := bloom_filter.GetCapacityFromErrorMargin(0.001, shared.FirstLevelMaxSize)
	s.bloomFilter = bloom_filter.NewBloomFilter(m, hashFunctions)

	for startIndex := uint64(0); startIndex < uint64(len(s.array)); startIndex += shared.BlockSize {
		endIndex := startIndex + shared.BlockSize

		value, err := shared.ByteToValue(s.array[startIndex+shared.KeySize : endIndex])
		if err != nil {
			return err
		}

		if !shared.IsTombstone(value) {
			keyByte := s.array[startIndex : startIndex+shared.KeySize]
			s.bloomFilter.Add(keyByte)
		}
	}

	return nil
}

// Write writes the data to the file including the metadata.
func (s *SSTable) Write() error {
	if s.osFile == nil {
		return FileNotOpenError
	}

	dataWithMetadata := append(s.metadata.ToByte(), s.array...)

	_, err := s.osFile.Write(dataWithMetadata)
	return err
}

// NewSSTable creates a new SSTable with the given path.
func NewSSTable() *SSTable {
	return &SSTable{}
}
