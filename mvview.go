package block_stm

// MVMemoryView wraps `MVMemory` for execution of a single transaction.
type MVMemoryView struct {
	storage KVStore

	txn      TxnIndex
	mvMemory *MVMemory
	readSet  ReadSet
	writeSet WriteSet
}

var _ KVStore = (*MVMemoryView)(nil)

func NewMVMemoryView(storage KVStore, mvMemory *MVMemory, txn TxnIndex) *MVMemoryView {
	return &MVMemoryView{
		storage:  storage,
		txn:      txn,
		mvMemory: mvMemory,
		writeSet: NewWriteSet(),
	}
}

func (s *MVMemoryView) Get(key Key) (Value, error) {
	if value := s.writeSet.MustGet(key); value != nil {
		// value written by this txn
		return value, nil
	}

	value, version, err := s.mvMemory.Read(key, s.txn)
	if err != nil {
		// return `(READ_ERROR, blocking_txn_id)` from the VM.execute
		return nil, err
	}

	if value == nil {
		// record version ⊥ when reading from storage
		s.readSet = append(s.readSet, ReadDescriptor{key, InvalidTxnVersion})
		return s.storage.Get(key)
	}

	s.readSet = append(s.readSet, ReadDescriptor{key, version})
	return value, nil
}

func (s *MVMemoryView) Set(key Key, value Value) error {
	s.writeSet.MustSet(key, value)
	return nil
}

func (s *MVMemoryView) VMResult() *VMResult {
	return &VMResult{
		ReadSet:  s.readSet,
		WriteSet: s.writeSet,
	}
}
