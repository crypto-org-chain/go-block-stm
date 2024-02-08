package block_stm

type VMResult struct {
	ReadSet  ReadSet
	WriteSet WriteSet
}

type KVStore interface {
	Get(Key) (Value, error)
	Set(Key, Value) error
}

type Tx func(KVStore) error

type VM struct {
	storage  KVStore
	mvMemory *MVMemory
	txs      []Tx
}

func NewVM(storage KVStore, mvMemory *MVMemory, txs []Tx) *VM {
	return &VM{
		storage:  storage,
		mvMemory: mvMemory,
		txs:      txs,
	}
}

func (vm *VM) Execute(txn TxnIndex) (*VMResult, error) {
	view := NewMVMemoryView(vm.storage, vm.mvMemory, txn)
	err := vm.txs[txn](view)
	if err != nil {
		return nil, err
	}

	return view.VMResult(), nil
}
