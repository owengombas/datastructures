package key_value

import "dmds_lab2/shared"

type KeyValueStore interface {
	Update(key shared.KeyType, value shared.ValueType) error
	Get(key shared.KeyType) (*shared.ValueType, error)
	Insert(key shared.KeyType, value shared.ValueType) error
	Delete(key shared.KeyType) error
}
