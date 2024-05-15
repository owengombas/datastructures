package shared

import "unsafe"

// KeyType is the type of the key.
type KeyType = uint64

// ValueType is the type of the value.
type ValueType = uint64

// KeySize is the size of the key type in bytes.
const KeySize = uint64(unsafe.Sizeof(KeyType(0)))

// ValueSize is the size of the value type in bytes.
const ValueSize = uint64(unsafe.Sizeof(ValueType(0)))
