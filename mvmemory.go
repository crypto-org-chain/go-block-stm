package block_stm

import "sync/atomic"

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

	wroteNewLocation := mv.rcuUpdateWrittenLocations(version.Index, newLocations)
	mv.lastReadSet[version.Index].Store(&readSet)
	return wroteNewLocation
}

// newLocations are sorted
func (mv *MVMemory) rcuUpdateWrittenLocations(txn TxnIndex, newLocations []Key) bool {
	prevLocations := mv.readLastWrittenLocations(txn)

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
	for _, key := range mv.readLastWrittenLocations(txn) {
		mv.data.WriteEstimate(key, txn)
	}
}

func (mv *MVMemory) Read(key Key, txn TxnIndex) (Value, TxnVersion, bool) {
	return mv.data.Read(key, txn)
}

func (mv *MVMemory) ValidateReadSet(txn TxnIndex) bool {
	// Invariant: at least one `Record` call has been made for `txn`
	readSet := *mv.lastReadSet[txn].Load()
	for _, desc := range readSet {
		_, version, estimate := mv.Read(desc.key, txn)
		if estimate {
			// previously read entry from data, now ESTIMATE
			return false
		}
		if version != desc.version {
			// previously read entry from data, now NOT_FOUND,
			// or read some entry, but not the same version as before
			return false
		}
	}
	return true
}

func (mv *MVMemory) readLastWrittenLocations(txn TxnIndex) []Key {
	p := mv.lastWrittenLocations[txn].Load()
	if p != nil {
		return *p
	}
	return nil
}

func (mv *MVMemory) Snapshot() []KVPair {
	return mv.data.Snapshot()
}
