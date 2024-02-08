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
	var hint PathHint
	writeSet.Scan(func(key Key, value Value) bool {
		mv.data.Write(key, value, version, &hint)
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

	var (
		hint             PathHint
		wroteNewLocation bool
	)
	DiffOrderedList(prevLocations, newLocations, func(key Key, is_new bool) bool {
		if is_new {
			wroteNewLocation = true
		} else {
			mv.data.Delete(key, txn, &hint)
		}
		return true
	})

	mv.lastWrittenLocations[txn].Store(&newLocations)
	return wroteNewLocation
}

func (mv *MVMemory) ConvertWritesToEstimates(txn TxnIndex) {
	var hint PathHint
	for _, key := range mv.readLastWrittenLocations(txn) {
		mv.data.WriteEstimate(key, txn, &hint)
	}
}

func (mv *MVMemory) Read(key Key, txn TxnIndex) (Value, TxnVersion, error) {
	return mv.data.Read(key, txn)
}

func (mv *MVMemory) ValidateReadSet(txn TxnIndex) bool {
	readSet := mv.readLastReadSet(txn)
	for _, desc := range readSet {
		_, version, err := mv.Read(desc.key, txn)
		switch err {
		case ErrNotFound:
			if desc.version.Valid() {
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

func (mv *MVMemory) readLastWrittenLocations(txn TxnIndex) []Key {
	p := mv.lastWrittenLocations[txn].Load()
	if p != nil {
		return *p
	}
	return nil
}

func (mv *MVMemory) readLastReadSet(txn TxnIndex) ReadSet {
	p := mv.lastReadSet[txn].Load()
	if p != nil {
		return *p
	}
	return nil
}
