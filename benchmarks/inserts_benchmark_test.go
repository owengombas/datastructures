package benchmarks

import (
	"bufio"
	"dmds_lab2/b_plus_tree"
	"dmds_lab2/shared"
	"fmt"
	"math/rand"
	"os"
	"testing"
)

func generateRandomKeys(N int) []shared.KeyType {
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

func saveKeysToCSV(keys []shared.KeyType) error {
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

func sequentialInserts(keys []shared.KeyType) (*b_plus_tree.BPlusTree, []shared.KeyType, error) {
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

func sequentialLookups(alex *b_plus_tree.BPlusTree, keys []shared.KeyType) error {
	for i := 0; i < len(keys); i++ {
		_, _, _, err := alex.Get(keys[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func TestSequentialInserts1k(t *testing.T) {
	keys := generateRandomKeys(1_000)
	saveKeysToCSV(keys)
	index, keys, err := sequentialInserts(keys)
	if err != nil {
		t.Error(err)
	}
	err = sequentialLookups(index, keys)
	if err != nil {
		t.Error(err)
	}
}

func TestSequentialInserts10k(t *testing.T) {
	keys := generateRandomKeys(10_000)
	saveKeysToCSV(keys)
	index, keys, err := sequentialInserts(keys)
	if err != nil {
		t.Error(err)
	}
	err = sequentialLookups(index, keys)
	if err != nil {
		t.Error(err)
	}
}

func TestSequentialInserts100k(t *testing.T) {
	keys := generateRandomKeys(100_000)
	saveKeysToCSV(keys)
	index, keys, err := sequentialInserts(keys)
	if err != nil {
		t.Error(err)
	}
	err = sequentialLookups(index, keys)
	if err != nil {
		t.Error(err)
	}
}

func TestSequentialInserts1m(t *testing.T) {
	keys := generateRandomKeys(1_000_000)
	saveKeysToCSV(keys)
	index, keys, err := sequentialInserts(keys)
	if err != nil {
		t.Error(err)
	}
	err = sequentialLookups(index, keys)
	if err != nil {
		t.Error(err)
	}
}

func BenchmarkSequentialInserts1kTo1m(b *testing.B) {
	for i := 1_000; i <= 1_000_000; i *= 10 {
		keys := generateRandomKeys(i)
		b.Run(fmt.Sprintf("SequentialInserts_%d", i), func(b *testing.B) {
			b.ResetTimer()
			_, _, err := sequentialInserts(keys)
			if err != nil {
				b.Error(err)
			}
		})
	}
}

func BenchmarkSequentialLookup1kTo1m(b *testing.B) {
	for i := 1_000; i <= 1_000_000; i *= 10 {
		b.Run(fmt.Sprintf("SequentialInserts_%d", i), func(b *testing.B) {
			keys := generateRandomKeys(i)
			index, _, err := sequentialInserts(keys)
			if err != nil {
				b.Error(err)
			}

			b.ResetTimer()
			err = sequentialLookups(index, keys)
			if err != nil {
				b.Error(err)
			}
		})
	}
}
