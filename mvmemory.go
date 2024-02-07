package block_stm

import (
	"sync/atomic"

	"github.com/tidwall/btree"
)

type (
	Key   string
	Value []byte
)

type writeSetItem struct {
	key   Key
	value Value
}

func writeSetItemLess(a, b writeSetItem) bool {
	return a.key < b.key
}

type btreeItem struct {
	Key      Key
	Version  TxnVersion
	Value    []byte
	Estimate bool
}

func btreeItemLesser(a, b btreeItem) bool {
	return a.Key < b.Key || a.Version.Index < b.Version.Index
}

type WriteSet = *btree.BTreeG[writeSetItem]

type ReadDescriptor struct {
	key Key
	// invalid version means the key is read from storage
	version TxnVersion
}

// MVMemory implements `Algorithm 2 The MVMemory module`
type MVMemory struct {
	data *btree.BTreeG[btreeItem]
	// keys are sorted
	lastWrittenLocations []atomic.Pointer[[]Key]
	lastReadSet          []atomic.Pointer[[]ReadDescriptor]
}

func NewMVMemory(block_size int) *MVMemory {
	return &MVMemory{
		data:                 btree.NewBTreeG[btreeItem](btreeItemLesser),
		lastWrittenLocations: make([]atomic.Pointer[[]Key], block_size),
		lastReadSet:          make([]atomic.Pointer[[]ReadDescriptor], block_size),
	}
}

func (mv *MVMemory) Record(version TxnVersion, readSet []ReadDescriptor, writeSet WriteSet) bool {
	newLocations := make([]Key, 0, writeSet.Len())

	// apply_write_set
	writeSet.Scan(func(item writeSetItem) bool {
		mv.write(item.key, item.value, version)
		newLocations = append(newLocations, item.key)
		return true
	})

	wroteNewLocation := mv.RCUUpdateWrittenLocations(version.Index, newLocations)
	mv.lastReadSet[version.Index].Store(&readSet)
	return wroteNewLocation
}

func (mv *MVMemory) write(key Key, value Value, version TxnVersion) {
	mv.data.Set(btreeItem{Key: key, Version: version, Value: value})
}

// newLocations are sorted
func (mv *MVMemory) RCUUpdateWrittenLocations(txn TxnIndex, newLocations []Key) bool {
	prevLocations := *mv.lastWrittenLocations[txn].Load()

	var wroteNewLocation bool
	DiffOrderedList(prevLocations, newLocations, func(key Key, is_new bool) bool {
		if is_new {
			wroteNewLocation = true
		} else {
			mv.data.Delete(btreeItem{Key: key, Version: TxnVersion{Index: txn}})
		}
		return true
	})

	mv.lastWrittenLocations[txn].Store(&newLocations)
	return wroteNewLocation
}

func (mv *MVMemory) ConvertWritesToEstimates(txn TxnIndex) {
	for _, key := range *mv.lastWrittenLocations[txn].Load() {
		mv.data.Set(btreeItem{Key: key, Version: TxnVersion{Index: txn}, Estimate: true})
	}
}

func (mv *MVMemory) Read(key Key, txn TxnIndex) (Value, TxnVersion, error) {
	iter := mv.data.Iter()
	iter.Seek(btreeItem{Key: key, Version: TxnVersion{Index: txn}})
	iter.Prev()
	item := iter.Item()
	iter.Release()

	if item.Key != key {
		return nil, TxnVersion{}, ErrNotFound
	}
	if item.Estimate {
		return nil, TxnVersion{}, ErrReadError{BlockingTxn: item.Version.Index}
	}
	return item.Value, item.Version, nil
}

func (mv *MVMemory) ValidateReadSet(txn TxnIndex) bool {
	readSet := *mv.lastReadSet[txn].Load()
	for _, desc := range readSet {
		_, version, err := mv.Read(desc.key, txn)
		switch err {
		case ErrNotFound:
			if version.IsValid() {
				// previously read entry from data, now NOT_FOUND
				return false
			}
		case nil:
			if version != desc.version {
				// read some entry, but not the same as before
				return false
			}
		default:
			// must be ErrReadError
			// previously read entry from data, now ESTIMATE
			return false
		}
	}
	return true
}
