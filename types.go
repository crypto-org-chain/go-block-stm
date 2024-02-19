package block_stm

import "bytes"

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

type IterationDescriptor struct {
	// [Start, End) is the initial range of the iterator, an non-exhausted iteration has an effectively smaller range.
	Start, End Key
	Ascending  bool
	// Stop is not `nil` if the iteration is not exhausted and stops at a key before reaching the end of the range,
	// when replaying, it should also stops at the stop key.
	Stop Key
	// Reads is the list of keys that is observed by the iterator.
	Reads []ReadDescriptor
}

type ReadSet struct {
	Reads    []ReadDescriptor
	Iterates []IterationDescriptor
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
