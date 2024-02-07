package block_stm

type VMResult struct {
	ReadSet  []ReadDescriptor
	WriteSet WriteSet
}

type KVStore interface {
	Get([]byte) ([]byte, error)
	Set([]byte, []byte) error
}

type Tx func(KVStore) (*VMResult, error)

type VM struct {
	txs []Tx
}

func NewVM(txs []Tx) *VM {
	return &VM{
		txs: txs,
	}
}

func (vm *VM) Execute(txn TxnIndex) (*VMResult, error) {
	// TODO vmmemory
	return vm.txs[txn](nil)
}
