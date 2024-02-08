package block_stm

import (
	"sync/atomic"
)

// MVMemory implements `Algorithm 2 The MVMemory module`
type MVMemory struct {
	data MVData
	// keys are sorted
	lastWrittenLocations []atomic.Pointer[[]Key]
	lastReadSet          []atomic.Pointer[ReadSet]
}

func NewMVMemory(block_size int) *MVMemory {
	return &MVMemory{
		data:                 *NewMVData(),
		lastWrittenLocations: make([]atomic.Pointer[[]Key], block_size),
		lastReadSet:          make([]atomic.Pointer[ReadSet], block_size),
	}
}

func (mv *MVMemory) Record(version TxnVersion, readSet ReadSet, writeSet WriteSet) bool {
	newLocations := make([]Key, 0, writeSet.Len())

	// apply_write_set
	writeSet.Scan(func(key Key, value Value) bool {
		mv.data.Write(key, value, version)
		newLocations = append(newLocations, key)
		return true
	})

	wroteNewLocation := mv.RCUUpdateWrittenLocations(version.Index, newLocations)
	mv.lastReadSet[version.Index].Store(&readSet)
	return wroteNewLocation
}

// newLocations are sorted
func (mv *MVMemory) RCUUpdateWrittenLocations(txn TxnIndex, newLocations []Key) bool {
	prevLocations := *mv.lastWrittenLocations[txn].Load()

	var wroteNewLocation bool
	DiffOrderedList(prevLocations, newLocations, func(key Key, is_new bool) bool {
		if is_new {
			wroteNewLocation = true
		} else {
			mv.data.Delete(key, txn)
		}
		return true
	})

	mv.lastWrittenLocations[txn].Store(&newLocations)
	return wroteNewLocation
}

func (mv *MVMemory) ConvertWritesToEstimates(txn TxnIndex) {
	for _, key := range *mv.lastWrittenLocations[txn].Load() {
		mv.data.WriteEstimate(key, txn)
	}
}

func (mv *MVMemory) Read(key Key, txn TxnIndex) (Value, TxnVersion, error) {
	return mv.data.Read(key, txn)
}

func (mv *MVMemory) ValidateReadSet(txn TxnIndex) bool {
	readSet := *mv.lastReadSet[txn].Load()
	for _, desc := range readSet {
		_, version, err := mv.Read(desc.key, txn)
		switch err {
		case ErrNotFound:
			if version.Valid() {
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
