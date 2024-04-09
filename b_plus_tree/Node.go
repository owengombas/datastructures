package b_plus_tree

import (
	"dmds_lab2/shared"
)

type Node interface {
	Scan(key shared.KeyType) (uint64, error)
	IsFull() bool
}
