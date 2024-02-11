package block_stm

import (
	"bytes"
	"sync"
)

type MVData struct {
	sync.Mutex
	inner BTree[dataItem]
}

func NewMVData() *MVData {
	return &MVData{
		inner: *NewBTree[dataItem](dataItemLess),
	}
}

func (d *MVData) getTreeOrDefault(key Key) *BTree[secondaryDataItem] {
	return d.inner.GetOrDefault(dataItem{Key: key}, func() dataItem {
		return dataItem{Key: key, Tree: NewBTree[secondaryDataItem](secondaryDataItemLess)}
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
	outer, ok := d.inner.Get(dataItem{Key: key})
	if !ok {
		return nil, TxnVersion{}, nil
	}

	outer.Tree.Lock()
	defer outer.Tree.Unlock()

	iter := outer.Tree.Inner.Iter()
	if iter.Seek(secondaryDataItem{Index: txn}) {
		if !iter.Prev() {
			return nil, TxnVersion{}, nil
		}
	} else {
		if !iter.Last() {
			return nil, TxnVersion{}, nil
		}
	}

	item := iter.Item()
	if item.Estimate {
		return nil, TxnVersion{}, &ErrReadError{BlockingTxn: item.Index}
	}
	return item.Value, item.Version(), nil
}

func (d *MVData) Snapshot() []KVPair {
	var snapshot []KVPair

	d.Lock()
	d.inner.Scan(func(outer dataItem) bool {
		item, ok := outer.Tree.Max()
		if !ok {
			return true
		}

		if item.Estimate {
			return true
		}

		snapshot = append(snapshot, KVPair{Key: outer.Key, Value: item.Value})
		return true
	})
	d.Unlock()

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
	return a.Index < b.Index
}

func (item secondaryDataItem) Version() TxnVersion {
	return TxnVersion{Index: item.Index, Incarnation: item.Incarnation}
}
