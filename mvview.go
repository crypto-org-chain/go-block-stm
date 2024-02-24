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

func (s *MVMemoryView) Get(key []byte) []byte {
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

func (s *MVMemoryView) Has(key []byte) bool {
	return s.Get(key) != nil
}

func (s *MVMemoryView) Set(key, value []byte) {
	if value == nil {
		panic("nil value is not allowed")
	}
	s.writeSet.OverlaySet(key, value)
}

func (s *MVMemoryView) Delete(key []byte) {
	s.writeSet.OverlaySet(key, nil)
}

func (s *MVMemoryView) Iterator(start, end []byte) storetypes.Iterator {
	return s.iterator(IteratorOptions{Start: start, End: end, Ascending: true})
}

func (s *MVMemoryView) ReverseIterator(start, end []byte) storetypes.Iterator {
	return s.iterator(IteratorOptions{Start: start, End: end, Ascending: false})
}

func (s *MVMemoryView) iterator(opts IteratorOptions) storetypes.Iterator {
	mvIter := s.mvMemory.Iterator(opts, s.store, s.txn, s.waitFor)

	var parentIter, wsIter storetypes.Iterator
	if opts.Ascending {
		wsIter = s.writeSet.Iterator(opts.Start, opts.End)
		parentIter = s.storage.Iterator(opts.Start, opts.End)
	} else {
		wsIter = s.writeSet.ReverseIterator(opts.Start, opts.End)
		parentIter = s.storage.ReverseIterator(opts.Start, opts.End)
	}

	onClose := func(iter storetypes.Iterator) {
		reads := mvIter.Reads()

		var stopKey Key
		if iter.Valid() {
			stopKey = iter.Key()

			// if the iterator is not exhausted, the merge iterator may have read one more key which is not observed by
			// the caller, in that case we remove that read descriptor.
			if len(reads) > 0 {
				lastRead := reads[len(reads)-1].Key
				if BytesBeyond(lastRead, stopKey, opts.Ascending) {
					reads = reads[:len(reads)-1]
				}
			}
		}

		s.readSet.Iterators = append(s.readSet.Iterators, IteratorDescriptor{
			IteratorOptions: opts,
			Stop:            stopKey,
			Reads:           reads,
		})
	}

	// three-way merge iterator
	return NewCacheMergeIterator(
		NewCacheMergeIterator(parentIter, mvIter, opts.Ascending, nil),
		wsIter,
		opts.Ascending,
		onClose,
	)
}
