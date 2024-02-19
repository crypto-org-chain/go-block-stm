package block_stm

import storetypes "cosmossdk.io/store/types"

// MVMemoryView wraps `MVMemory` for execution of a single transaction.
type MVMemoryView struct {
	storage   KVStore
	mvMemory  *MVMemory
	scheduler *Scheduler
	store     int

	txn      TxnIndex
	readSet  ReadSet
	writeSet WriteSet
}

var _ KVStore = (*MVMemoryView)(nil)

func NewMVMemoryView(store int, storage KVStore, mvMemory *MVMemory, schedule *Scheduler, txn TxnIndex) *MVMemoryView {
	return &MVMemoryView{
		store:     store,
		storage:   storage,
		mvMemory:  mvMemory,
		scheduler: schedule,
		txn:       txn,
		writeSet:  NewWriteSet(),
	}
}

func (s *MVMemoryView) waitFor(txn TxnIndex) {
	cond := s.scheduler.WaitForDependency(s.txn, txn)
	if cond != nil {
		cond.Wait()
	}
}

func (s *MVMemoryView) Get(key Key) Value {
	if value, found := s.writeSet.OverlayGet(key); found {
		// value written by this txn
		// nil value means deleted
		return value
	}

	for {
		value, version, estimate := s.mvMemory.Read(s.store, key, s.txn)
		if estimate {
			// read ESTIMATE mark, wait for the blocking txn to finish
			s.waitFor(version.Index)
			continue
		}

		// record the read version, invalid version is ⊥.
		// if not found, record version ⊥ when reading from storage.
		s.readSet.Reads = append(s.readSet.Reads, ReadDescriptor{key, version})
		if !version.Valid() {
			return s.storage.Get(key)
		}
		return value
	}
}

func (s *MVMemoryView) Has(key Key) bool {
	return s.Get(key) != nil
}

func (s *MVMemoryView) Set(key Key, value Value) {
	if value == nil {
		panic("nil value is not allowed")
	}
	s.writeSet.OverlaySet(key, value)
}

func (s *MVMemoryView) Delete(key Key) {
	s.writeSet.OverlaySet(key, nil)
}

func (s *MVMemoryView) Iterator(start, end Key) storetypes.Iterator {
	return s.iterator(start, end, true)
}

func (s *MVMemoryView) ReverseIterator(start, end Key) storetypes.Iterator {
	return s.iterator(start, end, false)
}

func (s *MVMemoryView) iterator(start, end Key, ascending bool) storetypes.Iterator {
	mvIter := s.mvMemory.Iterator(start, end, ascending, s.store, s.txn, s.waitFor)

	var parentIter, wsIter storetypes.Iterator
	if ascending {
		wsIter = s.writeSet.Iterator(start, end)
		parentIter = s.storage.Iterator(start, end)
	} else {
		wsIter = s.writeSet.ReverseIterator(start, end)
		parentIter = s.storage.ReverseIterator(start, end)
	}

	onClose := func(iter storetypes.Iterator) {
		reads := mvIter.Reads()

		var stopKey Key
		if iter.Valid() {
			stopKey = iter.Key()

			// if the iterator is not exhausted, the merge iterator may have read one more key which is not observed by
			// caller, in that case we remove the last read descriptor.
			if len(reads) > 0 {
				lastRead := reads[len(reads)-1].Key
				if BytesBeyond(lastRead, stopKey, ascending) {
					reads = reads[:len(reads)-1]
				}
			}
		}

		s.readSet.Iterates = append(s.readSet.Iterates, IterationDescriptor{
			Start:     start,
			End:       end,
			Ascending: ascending,
			Stop:      stopKey,
			Reads:     reads,
		})
	}

	// three-way merge iterator
	return NewCacheMergeIterator(
		NewCacheMergeIterator(parentIter, mvIter, ascending, nil),
		wsIter,
		ascending,
		onClose,
	)
}
