package shared

import (
	"bufio"
	"errors"
	"fmt"
	"math/rand"
	"os"
)

// ByteToKey converts a byte slice to a KeyType.
func ByteToKey(data []byte) (KeyType, error) {
	if uint64(len(data)) != KeySize {
		return 0, errors.New("invalid data size")
	}

	key := Endianess.Uint64(data)
	return KeyType(key), nil
}

// ByteToValue converts a byte slice to a ValueType.
func ByteToValue(data []byte) (ValueType, error) {
	if uint64(len(data)) != ValueSize {
		return ValueType(0), errors.New("invalid data size")
	}

	value := Endianess.Uint64(data)
	return ValueType(value), nil
}

// ByteToKeyValue converts a byte slice to a key-value pair.
func ByteToKeyValue(data []byte) (KeyType, ValueType, error) {
	key, err := ByteToKey(data[:KeySize])
	if err != nil {
		return 0, ValueType(0), err
	}
	value, err := ByteToValue(data[KeySize:BlockSize])
	if err != nil {
		return 0, ValueType(0), err
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
	Endianess.PutUint64(data, value)
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
	return data == TombstoneValue
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

func RandomNotInSet(rng *rand.Rand, max int, set map[int]struct{}) int {
	for {
		value := rng.Intn(max)
		if _, ok := set[value]; !ok {
			return value
		}
	}
}

func ReadValuesFromFile(filePath string) ([]int, []int) {
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}

	keys := make([]int, 0)
	values := make([]int, 0)
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		key, value := 0, 0
		fmt.Sscanf(scanner.Text(), "%d %d", &key, &value)
		keys = append(keys, key)
		values = append(values, value)
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	return keys, values
}
