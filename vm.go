package block_stm

type VMResult struct {
	ReadSet  MultiReadSet
	WriteSet MultiWriteSet
	Err      error
}

type KVStore interface {
	Get(Key) Value
	Has(Key) bool
	// nil value is not allowed in `Set`
	Set(Key, Value)
	Delete(Key)
}

type MultiStore interface {
	GetKVStore(string) KVStore
}

type Tx func(MultiStore) error

type VM struct {
	stores    []string
	storage   MultiStore
	mvMemory  *MVMemory
	scheduler *Scheduler
	txs       []Tx
}

func NewVM(stores []string, storage MultiStore, mvMemory *MVMemory, scheduler *Scheduler, txs []Tx) *VM {
	return &VM{
		stores:    stores,
		storage:   storage,
		mvMemory:  mvMemory,
		scheduler: scheduler,
		txs:       txs,
	}
}

func (vm *VM) Execute(txn TxnIndex) VMResult {
	view := NewMultiMVMemoryView(vm.stores, vm.storage, vm.mvMemory, vm.scheduler, txn)
	err := vm.txs[txn](view)
	readSet, writeSet := view.Result()
	return VMResult{
		ReadSet:  readSet,
		WriteSet: writeSet,
		Err:      err,
	}
}
