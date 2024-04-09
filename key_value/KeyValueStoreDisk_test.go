package key_value

import (
	"dmds_lab2/ss_table"
	"os"
	"testing"
)

func TestKeyValueStoreDisk_GetPath(t *testing.T) {
	// Initialize your KeyValueStoreDisk implementation
	// Initialize KeyValueStoreDisk with path != ""
	var kv KeyValueStoreDisk = ss_table.NewSSTable("test")

	// Test GetPath method
	path := kv.GetPath()
	if path == "" {
		t.Error("GetPath returned an empty string")
	}
}

func TestKeyValueStoreDisk_Create_not_exists(t *testing.T) {
	// Initialize your KeyValueStoreDisk implementation
	var kv KeyValueStoreDisk = ss_table.NewSSTable("test")

	// Test Create method
	err := kv.Create()
	defer func(name string) {
		err := os.Remove(name)
		if err != nil {
			panic(err)
		}
	}(kv.GetPath())
	if err != nil {
		t.Errorf("Create failed: %v", err)
	}

	// Check if the file exists
	_, err = os.Stat(kv.GetPath())
	if err != nil {
		t.Errorf("Create failed: %v", err)
	}
}

func TestKeyValueStoreDisk_Create_exists(t *testing.T) {
	// Initialize your KeyValueStoreDisk implementation
	var kv KeyValueStoreDisk = ss_table.NewSSTable("test")

	// Create the file first
	_, err := os.Create(kv.GetPath())
	defer func(name string) {
		err := os.Remove(name)
		if err != nil {
			panic(err)
		}
	}(kv.GetPath())

	// Test Create method
	err = kv.Create()
	if err == nil {
		t.Errorf("The create method should return an error if the file already exists")
	}
}

func TestKeyValueStoreDisk_Open_exists(t *testing.T) {
	// Initialize your KeyValueStoreDisk implementation
	var kv KeyValueStoreDisk = ss_table.NewSSTable("test")

	// Create the file first
	_, err := os.Create(kv.GetPath())
	defer func(name string) {
		err := os.Remove(name)
		if err != nil {
			panic(err)
		}
	}(kv.GetPath())

	// Test Open method
	err = kv.Open()
	if err != nil {
		t.Errorf("Open failed: %v", err)
	}
}

func TestKeyValueStoreDisk_Open_not_exists(t *testing.T) {
	// Initialize your KeyValueStoreDisk implementation
	var kv KeyValueStoreDisk = ss_table.NewSSTable("test")

	// Test Open method
	err := kv.Open()
	if err == nil {
		t.Errorf("The open method should return an error if the file does not exist")
	}
}

func TestKeyValueStoreDisk_Close_exists(t *testing.T) {
	// Initialize your KeyValueStoreDisk implementation
	var kv KeyValueStoreDisk = ss_table.NewSSTable("test")

	err := kv.Create()
	defer func(name string) {
		err := os.Remove(name)
		if err != nil {
			panic(err)
		}
	}(kv.GetPath())
	if err != nil {
		t.Errorf("Create failed: %v", err)
	}

	// Test Close method
	err = kv.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestKeyValueStoreDisk_Close_not_open(t *testing.T) {
	// Initialize your KeyValueStoreDisk implementation
	var kv KeyValueStoreDisk = ss_table.NewSSTable("test")

	// Test Close method
	err := kv.Close()
	if err == nil {
		t.Errorf("The close method should return an error if the file is not open")
	}
}

func TestKeyValueStoreDisk_Delete_exists(t *testing.T) {
	// Initialize your KeyValueStoreDisk implementation
	var kv KeyValueStoreDisk = ss_table.NewSSTable("test")

	err := kv.Create()
	if err != nil {
		t.Errorf("Create failed: %v", err)
	}

	// Test Delete method
	err = kv.Delete()
	if err != nil {
		t.Errorf("Delete failed: %v", err)
	}

	// Check if the file exists
	_, err = os.Stat(kv.GetPath())
	if err == nil {
		t.Errorf("Delete failed: the file still exists")
	}
}

func TestKeyValueStoreDisk_Delete_not_open(t *testing.T) {
	// Initialize your KeyValueStoreDisk implementation
	var kv KeyValueStoreDisk = ss_table.NewSSTable("test")

	// Test Delete method
	err := kv.Delete()
	if err == nil {
		t.Errorf("The delete method should return an error if the file is not open")
	}
}
