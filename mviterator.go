package block_stm

import (
	storetypes "cosmossdk.io/store/types"
	"github.com/tidwall/btree"
)

// MVIterator is an iterator for a multi-versioned store.
type MVIterator struct {
	BTreeIteratorG[dataItem]
	txn TxnIndex

	// cache current found value and version
	value   []byte
	version TxnVersion

	// record the observed reads during iteration during execution
	reads []ReadDescriptor
	// blocking call to wait for dependent transaction to finish, `nil` in validation mode
	waitFn func(TxnIndex)
	// signal the validation to fail
	readEstimateValue bool
}

var _ storetypes.Iterator = (*MVIterator)(nil)

func NewMVIterator(
	opts IteratorOptions, txn TxnIndex, iter btree.IterG[dataItem],
	waitFn func(TxnIndex),
) *MVIterator {
	it := &MVIterator{
		BTreeIteratorG: *NewBTreeIteratorG(
			dataItem{Key: opts.Start},
			dataItem{Key: opts.End},
			iter,
			opts.Ascending,
		),
		txn:    txn,
		waitFn: waitFn,
	}
	it.resolveValue()
	return it
}

// Executing returns if the iterator is running in execution mode.
func (it *MVIterator) Executing() bool {
	return it.waitFn != nil
}

func (it *MVIterator) Next() {
	it.BTreeIteratorG.Next()
	it.resolveValue()
}

func (it *MVIterator) Value() []byte {
	return it.value
}

func (it *MVIterator) Version() TxnVersion {
	return it.version
}

func (it *MVIterator) Reads() []ReadDescriptor {
	return it.reads
}

func (it *MVIterator) ReadEstimateValue() bool {
	return it.readEstimateValue
}

// resolveValue skips the non-exist values in the iterator based on the txn index, and caches the first existing one.
func (it *MVIterator) resolveValue() {
	inner := &it.BTreeIteratorG
	for ; inner.Valid(); inner.Next() {
		v, ok := it.resolveValueInner(inner.Item().Tree)
		if !ok {
			// abort the iterator
			it.valid = false
			// signal the validation to fail
			it.readEstimateValue = true
			return
		}
		if v == nil {
			continue
		}

		it.value = v.Value
		it.version = v.Version()
		if it.Executing() {
			it.reads = append(it.reads, ReadDescriptor{
				Key:     inner.Item().Key,
				Version: it.version,
			})
		}
		return
	}
}

// resolveValueInner loop until we find a value that is not an estimate,
// wait for dependency if gets an ESTIMATE.
// returns:
// - (nil, true) if the value is not found
// - (nil, false) if the value is an estimate and we should fail the validation
// - (v, true) if the value is found
func (it *MVIterator) resolveValueInner(tree *BTree[secondaryDataItem]) (*secondaryDataItem, bool) {
	for {
		v, ok := seekClosestTxn(tree, it.txn)
		if !ok {
			return nil, true
		}

		if v.Estimate {
			if it.Executing() {
				it.waitFn(v.Index)
				continue
			}
			// in validation mode, it should fail validation immediatelly
			return nil, false
		}

		return &v, true
	}
}
