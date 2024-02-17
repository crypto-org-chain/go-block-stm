package block_stm

// MVMemoryView wraps `MVMemory` for execution of a single transaction.
type MVMemoryView struct {
	storage   KVStore
	mvMemory  *MVMemory
	scheduler *Scheduler

	txn      TxnIndex
	readSet  ReadSet
	writeSet WriteSet
}

var _ KVStore = (*MVMemoryView)(nil)

func NewMVMemoryView(storage KVStore, mvMemory *MVMemory, schedule *Scheduler, txn TxnIndex) *MVMemoryView {
	return &MVMemoryView{
		storage:   storage,
		mvMemory:  mvMemory,
		scheduler: schedule,
		txn:       txn,
		writeSet:  NewWriteSet(),
	}
}

func (s *MVMemoryView) Get(key Key) Value {
	if value, found := s.writeSet.OverlayGet(key); found {
		// value written by this txn
		// nil value means deleted
		return value
	}

	for {
		value, version, estimate := s.mvMemory.Read(key, s.txn)
		if estimate {
			// read ESTIMATE mark, wait for the blocking txn to finish
			cond := s.scheduler.WaitForDependency(s.txn, version.Index)
			if cond != nil {
				cond.Wait()
			}
			continue
		}

		// record the read version, invalid version is ⊥.
		// if not found, record version ⊥ when reading from storage.
		s.readSet = append(s.readSet, ReadDescriptor{key, version})
		if version.Valid() {
			return value
		}
		return s.storage.Get(key)
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

func (s *MVMemoryView) Result() (ReadSet, WriteSet) {
	return s.readSet, s.writeSet
}
