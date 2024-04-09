package key_value

type KeyValueStoreDisk interface {
	GetPath() string
	Create() error
	Open() error
	Close() error
	Delete() error
}
