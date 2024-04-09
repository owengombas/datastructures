package hash_function

import "hash/fnv"

type FNVHashFunction struct{}

func (hf *FNVHashFunction) GetHash(key []byte) (uint64, error) {
	h := fnv.New64a()
	_, err := h.Write(key)
	if err != nil {
		return 0, err
	}
	return h.Sum64(), nil
}
