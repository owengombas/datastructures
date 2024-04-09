package shared

import "errors"

// KeyNotFoundError is the error returned when the key is not found.
var KeyNotFoundError = errors.New("key not found")

// KeyTombstonedError is the error returned when the key is tombstoned.
var KeyTombstonedError = errors.New("key is tombstoned")

// LevelFullError is the error returned when the level is full in the LSM Tree.
var LevelFullError = errors.New("level is full")
