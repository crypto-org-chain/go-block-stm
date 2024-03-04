package block_stm

import (
	"bytes"

	storetypes "cosmossdk.io/store/types"
)

type (
	TxnIndex    int
	Incarnation uint
)

type TxnVersion struct {
	Index       TxnIndex
	Incarnation Incarnation
}

var InvalidTxnVersion = TxnVersion{-1, 0}

func (v TxnVersion) Valid() bool {
	return v.Index >= 0
}

type (
	Key   []byte
	Value []byte
)

type ReadDescriptor struct {
	Key Key
	// invalid Version means the key is read from storage
	Version TxnVersion
}

type IteratorOptions struct {
	// [Start, End) is the range of the iterator
	Start     Key
	End       Key
	Ascending bool
}

type IteratorDescriptor struct {
	IteratorOptions
	// Stop is not `nil` if the iteration is not exhausted and stops at a key before reaching the end of the range,
	// the effective range is `[start, stop]`.
	// when replaying, it should also stops at the stop key.
	Stop Key
	// Reads is the list of keys that is observed by the iterator.
	Reads []ReadDescriptor
}

type ReadSet struct {
	Reads     []ReadDescriptor
	Iterators []IteratorDescriptor
}

type WriteSet = MemDB

func NewWriteSet() WriteSet {
	return *NewMemDBNonConcurrent()
}

type (
	MultiWriteSet = []WriteSet
	MultiReadSet  = []ReadSet
)

type KeyItem interface {
	GetKey() []byte
}

func KeyItemLess[T KeyItem](a, b T) bool {
	return bytes.Compare(a.GetKey(), b.GetKey()) < 0
}

// TxExecutor executes transactions on top of a multi-version memory view.
type TxExecutor func(TxnIndex, MultiStore)

type KVStore interface {
	storetypes.BasicKVStore
	Iterator(start, end []byte) storetypes.Iterator
	ReverseIterator(start, end []byte) storetypes.Iterator
}

type MultiStore interface {
	GetKVStore(storetypes.StoreKey) KVStore
}
