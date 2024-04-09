package hash_function

type HashFunction interface {
	GetHash(key []byte) (uint64, error)
}
