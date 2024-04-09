package bloom_filter

import (
	"dmds_lab2/hash_function"
	"math"
)

type BloomFilter struct {
	capacity      uint64
	bitmap        []bool
	hashFunctions []hash_function.HashFunction
	count         uint64
}

// GetOptimalNumberOfHasFunctions returns the optimal number of hash functions to use
func GetOptimalNumberOfHashFunctions(capacity uint64, nStoredElements uint64) uint64 {
	m := float64(capacity)
	n := float64(nStoredElements)
	return uint64(math.Ceil(m / n * math.Ln2))
}

// GetCapacityFromErrorMargin returns the number of bits needed to achieve a given error margin
func GetCapacityFromErrorMargin(errorMargin float64, nStoredElements uint64) uint64 {
	n := float64(nStoredElements)
	return uint64(math.Ceil(-n * math.Log(errorMargin) / math.Ln2))
}

// Add adds a key to the bloom filter
func (bf *BloomFilter) Add(key []byte) {
	for _, hf := range bf.hashFunctions {
		hash, _ := hf.GetHash(key)
		bf.bitmap[hash%bf.capacity] = true
	}
	bf.count++
}

// Contains checks if the key is in the bloom filter
// if it returns false, the key is definitely not in the bloom filter
// if it returns true, the key MIGHT be in the bloom filter
func (bf *BloomFilter) Contains(key []byte) bool {
	for _, hf := range bf.hashFunctions {
		hash, _ := hf.GetHash(key)
		if !bf.bitmap[hash%bf.capacity] {
			return false
		}
	}
	return true
}

// GetErrorMargin returns the error margin of the bloom filter according to it's current number of element
func (bf *BloomFilter) GetErrorMargin() float64 {
	n := float64(bf.capacity)
	m := float64(bf.count)
	k := float64(len(bf.hashFunctions))
	return math.Pow(1-math.Exp(-k*n/m), k)
}

// NewBloomFilter creates a new bloom filter with a given number of bits
// the more space we allocate, the least hash function we can use
func NewBloomFilter(capacity uint64, hashFunctions []hash_function.HashFunction) *BloomFilter {
	if capacity <= 0 {
		panic("capacity must be greater than 0")
	}
	return &BloomFilter{
		capacity:      capacity,
		count:         0,
		bitmap:        make([]bool, capacity),
		hashFunctions: hashFunctions,
	}
}
