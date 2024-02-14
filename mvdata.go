package block_stm

import (
	"bytes"
)

type MVData struct {
	BTree[dataItem]
}

func NewMVData() *MVData {
	return &MVData{*NewBTree[dataItem](dataItemLess)}
}

func (d *MVData) getTree(key Key) *BTree[secondaryDataItem] {
	outer, _ := d.Get(dataItem{Key: key})
	return outer.Tree
}

func (d *MVData) getTreeOrDefault(key Key) *BTree[secondaryDataItem] {
	return d.GetOrDefault(dataItem{Key: key}, func(item *dataItem) {
		if item.Tree == nil {
			item.Tree = NewBTree[secondaryDataItem](secondaryDataItemLess)
		}
	}).Tree
}

func (d *MVData) Write(key Key, value Value, version TxnVersion) {
	tree := d.getTreeOrDefault(key)
	tree.Set(secondaryDataItem{Index: version.Index, Incarnation: version.Incarnation, Value: value})
}

func (d *MVData) WriteEstimate(key Key, txn TxnIndex) {
	tree := d.getTreeOrDefault(key)
	tree.Set(secondaryDataItem{Index: txn, Estimate: true})
}

func (d *MVData) Delete(key Key, txn TxnIndex) {
	tree := d.getTreeOrDefault(key)
	tree.Delete(secondaryDataItem{Index: txn})
}

func (d *MVData) Read(key Key, txn TxnIndex) (Value, TxnVersion, *ErrReadError) {
	if txn == 0 {
		return nil, TxnVersion{}, nil
	}

	tree := d.getTree(key)
	if tree == nil {
		return nil, TxnVersion{}, nil
	}

	// index order is reversed,
	// find the closing txn that's less than the given txn
	item, ok := tree.Seek(secondaryDataItem{Index: txn - 1})
	if !ok {
		return nil, TxnVersion{}, nil
	}

	if item.Estimate {
		return nil, TxnVersion{}, &ErrReadError{BlockingTxn: item.Index}
	}
	return item.Value, item.Version(), nil
}

func (d *MVData) Snapshot() []KVPair {
	var snapshot []KVPair

	d.Scan(func(outer dataItem) bool {
		// index order is reversed, `Min` is the latest
		item, ok := outer.Tree.Min()
		if !ok {
			return true
		}

		if item.Estimate {
			return true
		}

		snapshot = append(snapshot, KVPair{Key: outer.Key, Value: item.Value})
		return true
	})

	return snapshot
}

type KVPair struct {
	Key   Key
	Value Value
}

type dataItem struct {
	Key  Key
	Tree *BTree[secondaryDataItem]
}

func dataItemLess(a, b dataItem) bool {
	return bytes.Compare(a.Key, b.Key) < 0
}

type secondaryDataItem struct {
	Index       TxnIndex
	Incarnation Incarnation
	Value       []byte
	Estimate    bool
}

func secondaryDataItemLess(a, b secondaryDataItem) bool {
	// reverse the order
	return a.Index > b.Index
}

func (item secondaryDataItem) Version() TxnVersion {
	return TxnVersion{Index: item.Index, Incarnation: item.Incarnation}
}
