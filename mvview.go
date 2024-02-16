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
	if value := s.writeSet.Get(key); value != nil {
		// value written by this txn
		return value
	}

	for {
		value, version, err := s.mvMemory.Read(key, s.txn)
		if err != nil {
			// read ESTIMATE mark, wait for the blocking txn to finish
			cond := s.scheduler.WaitForDependency(s.txn, err.BlockingTxn)
			if cond != nil {
				cond.Wait()
			}
			continue
		}

		if value == nil {
			// record version ⊥ when reading from storage
			s.readSet = append(s.readSet, ReadDescriptor{key, InvalidTxnVersion})
			return s.storage.Get(key)
		}

		s.readSet = append(s.readSet, ReadDescriptor{key, version})
		return value
	}
}

func (s *MVMemoryView) Set(key Key, value Value) {
	s.writeSet.Set(key, value)
}

func (s *MVMemoryView) Result() (ReadSet, WriteSet) {
	return s.readSet, s.writeSet
}
