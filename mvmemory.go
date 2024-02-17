package block_stm

import "sync/atomic"

type (
	// keys are sorted
	Locations      []Key
	MultiLocations []Locations
)

// MVMemory implements `Algorithm 2 The MVMemory module`
type MVMemory struct {
	stores               []string
	data                 []MVData
	lastWrittenLocations []atomic.Pointer[MultiLocations]
	lastReadSet          []atomic.Pointer[MultiReadSet]
}

func NewMVMemory(block_size int, stores []string) *MVMemory {
	data := make([]MVData, len(stores))
	for i := 0; i < len(stores); i++ {
		data[i] = *NewMVData()
	}
	return &MVMemory{
		stores:               stores,
		data:                 data,
		lastWrittenLocations: make([]atomic.Pointer[MultiLocations], block_size),
		lastReadSet:          make([]atomic.Pointer[MultiReadSet], block_size),
	}
}

func (mv *MVMemory) Record(version TxnVersion, rs MultiReadSet, ws MultiWriteSet) bool {
	newLocations := make(MultiLocations, len(mv.stores))
	for i, writeSet := range ws {
		if writeSet.Len() == 0 {
			continue
		}

		newLocations[i] = make([]Key, 0, writeSet.Len())

		// apply_write_set
		writeSet.Scan(func(key Key, value Value) bool {
			mv.data[i].Write(key, value, version)
			newLocations[i] = append(newLocations[i], key)
			return true
		})
	}
	wroteNewLocation := mv.rcuUpdateWrittenLocations(version.Index, newLocations)
	mv.lastReadSet[version.Index].Store(&rs)
	return wroteNewLocation
}

// newLocations are sorted
func (mv *MVMemory) rcuUpdateWrittenLocations(txn TxnIndex, newLocations MultiLocations) bool {
	var wroteNewLocation bool

	prevLocations := mv.readLastWrittenLocations(txn)
	for i := range mv.stores {
		if i >= len(prevLocations) {
			// special case, prevLocations is not initialized
			if len(newLocations[i]) > 0 {
				wroteNewLocation = true
			}
			continue
		}

		DiffOrderedList(prevLocations[i], newLocations[i], func(key Key, is_new bool) bool {
			if is_new {
				wroteNewLocation = true
			} else {
				mv.data[i].Delete(key, txn)
			}
			return true
		})
	}
	mv.lastWrittenLocations[txn].Store(&newLocations)
	return wroteNewLocation
}

func (mv *MVMemory) ConvertWritesToEstimates(txn TxnIndex) {
	for i, locations := range mv.readLastWrittenLocations(txn) {
		for _, key := range locations {
			mv.data[i].WriteEstimate(key, txn)
		}
	}
}

func (mv *MVMemory) Read(store int, key Key, txn TxnIndex) (Value, TxnVersion, bool) {
	return mv.data[store].Read(key, txn)
}

func (mv *MVMemory) ValidateReadSet(txn TxnIndex) bool {
	// Invariant: at least one `Record` call has been made for `txn`
	rs := *mv.lastReadSet[txn].Load()
	for store, readSet := range rs {
		for _, desc := range readSet {
			_, version, estimate := mv.Read(store, desc.key, txn)
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
	}
	return true
}

func (mv *MVMemory) readLastWrittenLocations(txn TxnIndex) MultiLocations {
	p := mv.lastWrittenLocations[txn].Load()
	if p != nil {
		return *p
	}
	return nil
}

func (mv *MVMemory) WriteSnapshot(storage MultiStore) {
	for i, name := range mv.stores {
		WriteSnapshot(storage.GetKVStore(name), mv.data[i].Snapshot())
	}
}

func WriteSnapshot(storage KVStore, snapshot []KVPair) {
	for _, pair := range snapshot {
		if pair.Value == nil {
			storage.Delete(pair.Key)
		} else {
			storage.Set(pair.Key, pair.Value)
		}
	}
}
