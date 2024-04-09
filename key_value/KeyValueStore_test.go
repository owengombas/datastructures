package key_value

import (
	"dmds_lab2/shared"
	"dmds_lab2/skip_list"
	"testing"
)

func TestKeyValueStore_Insert(t *testing.T) {
	// Initialize your KeyValueStore implementation
	var kv KeyValueStore = skip_list.NewSkipList()
	// Assuming you have some test data
	key := shared.KeyType(1)
	value := shared.ValueType{255, 0, 0, 0, 0, 0, 0, 0}

	// Test Insert method
	err := kv.Insert(key, value)
	if err != nil {
		t.Errorf("Insert failed: %v", err)
	}

	valueRetrieved, err := kv.Get(key)
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}

	// Check if the retrieved value matches the inserted value
	if *valueRetrieved != value {
		t.Errorf("Expected value %v but got %v", value, *valueRetrieved)
	}
}

func TestKeyValueStore_Get(t *testing.T) {
	// Initialize your KeyValueStore implementation
	var kv KeyValueStore = skip_list.NewSkipList()
	// Assuming you have some test data
	key := shared.KeyType(1)
	expectedValue := shared.ValueType{255, 0, 0, 0, 0, 0, 0, 0}

	// Insert a test value before trying to retrieve it
	err := kv.Insert(key, expectedValue)
	if err != nil {
		t.Errorf("Insert failed: %v", err)
	}

	// Test Get method
	value, err := kv.Get(key)
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}

	// Check if the retrieved value matches the expected value
	if *value != expectedValue {
		t.Errorf("Expected value %s but got %s", expectedValue, *value)
	}
}

func TestKeyValueStore_Update(t *testing.T) {
	// Initialize your KeyValueStore implementation
	var kv KeyValueStore = skip_list.NewSkipList()
	// Assuming you have some test data
	key := shared.KeyType(1)
	value := shared.ValueType{255, 0, 0, 0, 0, 0, 0, 0}

	// Insert a test value before trying to update it
	err := kv.Insert(key, value)
	if err != nil {
		t.Errorf("Insert failed: %v", err)
	}

	// Test Update method
	err = kv.Update(key, value)
	if err != nil {
		t.Errorf("Update failed: %v", err)
	}

	valueRetrieved, err := kv.Get(key)
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}

	// Check if the retrieved value matches the updated value
	if *valueRetrieved != value {
		t.Errorf("Expected value %v but got %v", value, *valueRetrieved)
	}
}
