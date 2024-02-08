package block_stm

import (
	"github.com/tidwall/btree"
)

type PathHint = btree.PathHint

type MVData struct {
	inner btree.BTreeG[dataItem]
}

func NewMVData() *MVData {
	return &MVData{inner: *btree.NewBTreeG[dataItem](dataItemLess)}
}

func (d *MVData) Write(key Key, value Value, version TxnVersion, hint *PathHint) {
	d.inner.SetHint(dataItem{Key: key, Index: version.Index, Incarnation: version.Incarnation, Value: value}, hint)
}

func (d *MVData) WriteEstimate(key Key, txn TxnIndex, hint *PathHint) {
	d.inner.SetHint(dataItem{Key: key, Index: txn, Estimate: true}, hint)
}

func (d *MVData) Delete(key Key, txn TxnIndex, hint *PathHint) {
	d.inner.DeleteHint(dataItem{Key: key, Index: txn}, hint)
}

func (d *MVData) Read(key Key, txn TxnIndex) (Value, TxnVersion, error) {
	iter := d.inner.Iter()
	defer iter.Release()

	if iter.Seek(dataItem{Key: key, Index: txn}) {
		if !iter.Prev() {
			return nil, TxnVersion{}, ErrNotFound
		}
	}

	item := iter.Item()

	if item.Key != key {
		return nil, TxnVersion{}, ErrNotFound
	}
	if item.Estimate {
		return nil, TxnVersion{}, ErrReadError{BlockingTxn: item.Index}
	}
	return item.Value, item.Version(), nil
}

type dataItem struct {
	Key         Key
	Index       TxnIndex
	Incarnation Incarnation
	Value       []byte
	Estimate    bool
}

func dataItemLess(a, b dataItem) bool {
	return a.Key < b.Key || a.Index < b.Index
}

func (item dataItem) Version() TxnVersion {
	return TxnVersion{Index: item.Index, Incarnation: item.Incarnation}
}
