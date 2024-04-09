package ss_table

import (
	"dmds_lab2/shared"
	"unsafe"
)

type Metadata struct {
	minKey shared.KeyType
	maxKey shared.KeyType
}

const MetadataSize = uint64(unsafe.Sizeof(Metadata{}))

func (m *Metadata) GetMinKey() shared.KeyType {
	return m.minKey
}

func (m *Metadata) GetMaxKey() shared.KeyType {
	return m.maxKey
}

func (m *Metadata) ToByte() []byte {
	metadata := [MetadataSize]byte{}
	shared.Endianess.PutUint64(metadata[0:shared.KeySize], m.minKey)
	shared.Endianess.PutUint64(metadata[shared.KeySize:2*shared.KeySize], m.maxKey)
	return metadata[:]
}

func (m *Metadata) FromByte(data [MetadataSize]byte) {
	m.minKey = shared.Endianess.Uint64(data[0:shared.KeySize])
	m.maxKey = shared.Endianess.Uint64(data[shared.KeySize : 2*shared.KeySize])
}

func NewMetadata(minKey shared.KeyType, maxKey shared.KeyType) Metadata {
	return Metadata{
		minKey,
		maxKey,
	}
}
