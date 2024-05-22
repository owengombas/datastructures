package tests

import (
	"bufio"
	"dmds_lab2/b_plus_tree"
	"dmds_lab2/shared"
	"fmt"
	"math/rand"
	"os"
)

func GenerateRandomKeys(N int) []shared.KeyType {
	source := rand.NewSource(42)
	rng := rand.New(source)
	keys := make([]shared.KeyType, N)
	existingKeys := map[shared.KeyType]bool{}
	for i := 0; i < N; i++ {
		for {
			key := shared.KeyType(rng.Intn(N * 2))
			if _, ok := existingKeys[key]; !ok {
				keys[i] = key
				existingKeys[key] = true
				break
			}
		}
	}
	return keys
}

func SaveKeysToCSV(keys []shared.KeyType) error {
	file, err := os.Create(fmt.Sprintf("keys_%d.csv", len(keys)))
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	for i := 0; i < len(keys); i++ {
		_, err := writer.WriteString(fmt.Sprintf("%d\n", keys[i]))
		if err != nil {
			return err
		}
	}

	return nil
}

func SequentialInserts(keys []shared.KeyType) (*b_plus_tree.BPlusTree, []shared.KeyType, error) {
	alex := b_plus_tree.NewBPlusTree(1)

	for i := 0; i < len(keys); i++ {
		key := keys[i]
		_, _, err := alex.Insert(shared.KeyType(key))
		if err != nil {
			return alex, keys, err
		}
	}

	return alex, keys, nil
}

func SequentialLookups(alex *b_plus_tree.BPlusTree, keys []shared.KeyType) error {
	for i := 0; i < len(keys); i++ {
		_, _, _, err := alex.Get(keys[i])
		if err != nil {
			return err
		}
	}
	return nil
}
