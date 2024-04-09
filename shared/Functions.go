package shared

import (
	"errors"
	"math/rand"
)

// ByteToKey converts a byte slice to a KeyType.
func ByteToKey(data []byte) (KeyType, error) {
	if uint64(len(data)) != KeySize {
		return 0, errors.New("invalid data size")
	}

	key := Endianess.Uint64(data)
	return key, nil
}

// ByteToValue converts a byte slice to a ValueType.
func ByteToValue(data []byte) (ValueType, error) {
	if uint64(len(data)) != ValueSize {
		return ValueType{}, errors.New("invalid data size")
	}

	value := ValueType(data)
	return value, nil
}

// ByteToKeyValue converts a byte slice to a key-value pair.
func ByteToKeyValue(data []byte) (KeyType, ValueType, error) {
	key, err := ByteToKey(data[:KeySize])
	if err != nil {
		return 0, ValueType{}, err
	}
	value, err := ByteToValue(data[KeySize:BlockSize])
	if err != nil {
		return 0, ValueType{}, err
	}
	return key, value, nil
}

// KeyToByte converts a KeyType to a byte slice.
func KeyToByte(key KeyType) []byte {
	data := make([]byte, KeySize)
	Endianess.PutUint64(data, key)
	return data
}

// ValueToByte converts a ValueType to a byte slice.
func ValueToByte(value ValueType) []byte {
	data := make([]byte, ValueSize)
	copy(data, value[:])
	return data
}

// KeyValueToByte converts a key-value pair to a byte slice.
func KeyValueToByte(key KeyType, value ValueType) []byte {
	data := make([]byte, BlockSize)
	copy(data, KeyToByte(key))
	copy(data[KeySize:], ValueToByte(value))
	return data
}

// IsTombstone returns true if the data is a tombstone.
func IsTombstone(data ValueType) bool {
	for _, b := range data {
		if b != 255 {
			return false
		}
	}
	return true
}

// RandomString generates a random string of length n.
// Source: https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-go
func RandomString(n int) string {
	var letterByte = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	b := make([]byte, n)
	for i := range b {
		b[i] = letterByte[rand.Int63()%int64(len(letterByte))]
	}
	return string(b)
}
