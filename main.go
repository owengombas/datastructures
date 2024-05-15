package main

import (
	"dmds_lab2/b_plus_tree"
	"dmds_lab2/lsm_tree"
	"dmds_lab2/shared"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
)

const rootDirectory = ".ss_tables"
const MaxLevelTest = 15

func testBPlusTree() {
	r := rand.New(rand.NewSource(4))
	ln := b_plus_tree.NewBPlusTree(3)
	exists := make(map[uint64]bool)
	for i := 0; i < 100; i++ {
		n := r.Intn(100)
		doesExist := exists[uint64(n)]
		if !doesExist {
			fmt.Println(n)
			ln.Insert(shared.KeyType(n))
		}
		exists[uint64(n)] = true
	}
	leaf, _, _, _ := ln.Get(1)

	for leaf != nil {
		for _, k := range leaf.GetKeys() {
			if k != 0 {
				fmt.Print(strconv.FormatUint(k, 10), " ")
			} else {
				fmt.Print(" . ")
			}
		}
		fmt.Print(" -> ")
		leaf = leaf.GetNext()
	}
}

func testRetrieval(expectedValues map[shared.KeyType]shared.ValueType, lsmTree *lsm_tree.LSMTree) {
	for expectedKey, expectedValue := range expectedValues {
		value, _, sourceFile, err := lsmTree.Get(expectedKey)

		if errors.Is(err, shared.KeyNotFoundError) {
			fmt.Printf("------>  🕳️ key-value not found %d\n", expectedKey)
			continue
		}

		if errors.Is(err, shared.KeyTombstonedError) {
			fmt.Printf("------> 🪦 The key %d has been tombstoned\n", expectedKey)
			continue
		}

		if err != nil {
			fmt.Printf("------> ❌ Failed to retrieve key-value %d from %s: %v\n", expectedKey, sourceFile, err)
			continue
		}

		if *value != expectedValue {
			fmt.Printf("------>  🧐 The expected value of %d do not match: retrieved=%d, expected=%d (from %s)\n", expectedKey, value, expectedValue, sourceFile)
		} else {
			fmt.Printf("✅ Successfully retrieved key-value %d: %d from %s\n", expectedKey, value, sourceFile)
		}
	}
}

func createLSMTree(keyValueStore map[shared.KeyType]shared.ValueType) *lsm_tree.LSMTree {
	// rootDirectory is the directory where the SSTable files are stored.
	// extension is the extension of the SSTable files.

	// Empty out the .ss_tables directory. and the .cache directory.
	if err := os.RemoveAll(rootDirectory); err != nil {
		panic(err)
	}

	// Create the .ss_tables directory and the .cache directory.
	if err := os.MkdirAll(rootDirectory, 0755); err != nil {
		panic(err)
	}

	lsmTree, err := lsm_tree.NewLSMTree(rootDirectory, MaxLevelTest)
	if err != nil {
		panic(err)
	}

	// add +2 to N*shared.FirstLevelMaxSize aiming to check if the cache file stores the key-value pairs that are not yet written to the SSTable file.
	i := 0
	for key, value := range keyValueStore {
		fmt.Printf("%d) Inserting key-value %d: %d\n", i+1, key, value)
		err := lsmTree.Insert(key, value)
		if err != nil {
			panic(err)
		}
		i++
	}

	return lsmTree
}

func loadLSMTree() *lsm_tree.LSMTree {
	fmt.Println("====== Retrieval after loading from disk =======")

	lsmTree, err := lsm_tree.NewLSMTree(rootDirectory, MaxLevelTest)
	if err != nil {
		panic(err)
	}

	return lsmTree
}

func mainLSMTree() {
	const nValues = 500 * shared.FirstLevelMaxSize
	randomValuesGenerator := rand.New(rand.NewSource(123))
	keyValueStore := make(map[shared.KeyType]shared.ValueType)
	for i := uint64(0); i < nValues; i++ {
		key := randomValuesGenerator.Uint64()
		keyValueStore[key] = i
	}

	const nValuesNotExisting = 100
	notExistingKeyValueStore := make(map[shared.KeyType]shared.ValueType)
	for i := uint64(0); i < nValuesNotExisting; i++ {
		key := randomValuesGenerator.Uint64()
		notExistingKeyValueStore[key] = i
	}

	fmt.Println("====== Retrieval right after creation =======")
	lsmTree := createLSMTree(keyValueStore)
	testRetrieval(keyValueStore, lsmTree)
	testRetrieval(notExistingKeyValueStore, lsmTree)

	fmt.Println("====== Retrieval after loading from disk =======")
	lsmTree = loadLSMTree()
	testRetrieval(keyValueStore, lsmTree)
	testRetrieval(notExistingKeyValueStore, lsmTree)
}

func main() {
	mainLSMTree()
}
