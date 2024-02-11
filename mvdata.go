package block_stm

import (
	"bytes"
	"sync"

	"github.com/tidwall/btree"
)

type MVData struct {
	sync.RWMutex
	inner btree.BTreeG[dataItem]
}

func NewMVData() *MVData {
	return &MVData{
		inner: *btree.NewBTreeGOptions[dataItem](dataItemLess, btree.Options{
			// concurrency unsafe tree, protected by custom mutex
			NoLocks: true,
		})}
}

// getTreeOrDefault returns the tree for the given key, creates a new tree if the key is not present
func (d *MVData) getTreeOrDefault(key Key) *btree.BTreeG[secondaryDataItem] {
	var tree *btree.BTreeG[secondaryDataItem]

	d.Lock()
	item, ok := d.inner.Get(dataItem{Key: key})
	if !ok {
		// concurrency safe tree
		tree = btree.NewBTreeG[secondaryDataItem](secondaryDataItemLess)
		d.inner.Set(dataItem{Key: key, Tree: tree})
	} else {
		tree = item.Tree
	}

	d.Unlock()
	return tree
}

// getTree returns the tree for the given key, returns nil if the key is not present
func (d *MVData) getTree(key Key) *btree.BTreeG[secondaryDataItem] {
	d.RLock()
	item, _ := d.inner.Get(dataItem{Key: key})
	d.RUnlock()
	return item.Tree
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
	tree.Set(secondaryDataItem{Index: txn, Estimate: true})
	tree.Delete(secondaryDataItem{Index: txn})
}

func (d *MVData) Read(key Key, txn TxnIndex) (Value, TxnVersion, *ErrReadError) {
	tree := d.getTree(key)
	if tree == nil {
		return nil, TxnVersion{}, nil
	}

	iter := tree.Iter()
	defer iter.Release()

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

	d.RLock()
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
	d.RUnlock()

	return snapshot
}

type KVPair struct {
	Key   Key
	Value Value
}

type dataItem struct {
	Key  Key
	Tree *btree.BTreeG[secondaryDataItem]
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
