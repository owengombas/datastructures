package shared

import (
	"dmds_lab2/hash_function"
	"encoding/binary"
	"math"
	"math/rand"
)

// RandomGenerator is the random number generator at a given seed.
var RandomGenerator = rand.New(rand.NewSource(1))

// BlockSize is the size of a block in the SSTable (KeySize + ValueSize).
const BlockSize = KeySize + ValueSize

// Endianess is the endianess used for encoding and decoding
var Endianess = binary.LittleEndian

// TombstoneValue is the value used to mark a key as deleted.
var TombstoneValue = ValueType(math.MaxUint64)

const SSTablesRootDirectory = ".ss_tables"

// FirstLevelMaxSize represent the maximum size before compacting the SkipList to SSTable
// Every component in the LSM Tree has a FirstLevelMaxSize number of key-value pairs.
const FirstLevelMaxSize uint64 = 3

// GrowthFactor is the factor used to determine the level of the SkipList.
const GrowthFactor = 2

// SkipListExtension is the extension used for the SkipList files.
const SkipListExtension = ".sl"

// SSTableExtension is the extension used for the SSTable files.
const SSTableExtension = ".sst"

var HashFunctions = []hash_function.HashFunction{&hash_function.FNVHashFunction{}, &hash_function.MD5HashFunction{}}
