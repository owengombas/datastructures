package hash_function

import "crypto/md5"

type MD5HashFunction struct {
}

func (hf *MD5HashFunction) GetHash(key []byte) (uint64, error) {
	md5Hash, err := md5.New().Write(key)

	return uint64(md5Hash), err
}
