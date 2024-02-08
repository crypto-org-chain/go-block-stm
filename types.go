package block_stm

type (
	TxnIndex    int
	Incarnation uint
)

type TxnVersion struct {
	Index       TxnIndex
	Incarnation Incarnation
}

var InvalidTxnVersion = TxnVersion{-1, 0}

func (v TxnVersion) Valid() bool {
	return v.Index >= 0
}

type (
	Key   string
	Value []byte
)

type ReadDescriptor struct {
	key Key
	// invalid version means the key is read from storage
	version TxnVersion
}

type ReadSet []ReadDescriptor

type WriteSet = MemDB

func NewWriteSet() WriteSet {
	return *NewMemDBNonConcurrent()
}
