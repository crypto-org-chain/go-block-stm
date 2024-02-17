package block_stm

type VMResult struct {
	ReadSet  ReadSet
	WriteSet WriteSet
	Err      error
}

type KVStore interface {
	Get(Key) Value
	Has(Key) bool
	// nil value is not allowed in `Set`
	Set(Key, Value)
	Delete(Key)
}

type Tx func(KVStore) error

type VM struct {
	storage   KVStore
	mvMemory  *MVMemory
	scheduler *Scheduler
	txs       []Tx
}

func NewVM(storage KVStore, mvMemory *MVMemory, scheduler *Scheduler, txs []Tx) *VM {
	return &VM{
		storage:   storage,
		mvMemory:  mvMemory,
		scheduler: scheduler,
		txs:       txs,
	}
}

func (vm *VM) Execute(txn TxnIndex) VMResult {
	view := NewMVMemoryView(vm.storage, vm.mvMemory, vm.scheduler, txn)
	err := vm.txs[txn](view)
	readSet, writeSet := view.Result()
	return VMResult{
		ReadSet:  readSet,
		WriteSet: writeSet,
		Err:      err,
	}
}
