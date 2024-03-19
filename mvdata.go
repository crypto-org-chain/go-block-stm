package block_stm

import (
	"bytes"
)

const (
	// Since we do copy-on-write a lot, smaller degree means smaller allocations
	OuterBTreeDegree = 4
	InnerBTreeDegree = 4
)

type MVData struct {
	BTree[dataItem]
}

func NewMVData() *MVData {
	return &MVData{*NewBTree(KeyItemLess[dataItem], OuterBTreeDegree)}
}

// getTree returns `nil` if not found
func (d *MVData) getTree(key Key) *BTree[secondaryDataItem] {
	outer, _ := d.Get(dataItem{Key: key})
	return outer.Tree
}

// getTreeOrDefault set a new tree atomically if not found.
func (d *MVData) getTreeOrDefault(key Key) *BTree[secondaryDataItem] {
	return d.GetOrDefault(dataItem{Key: key}, func(item *dataItem) {
		if item.Tree == nil {
			item.Tree = NewBTree(secondaryLesser, InnerBTreeDegree)
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

// Read returns the value and the version of the value that's less than the given txn.
// If the key is not found, returns `(nil, InvalidTxnVersion, false)`.
// If the key is found but value is an estimate, returns `(nil, BlockingTxn, true)`.
// If the key is found, returns `(value, version, false)`, `value` can be `nil` which means deleted.
func (d *MVData) Read(key Key, txn TxnIndex) (Value, TxnVersion, bool) {
	if txn == 0 {
		return nil, InvalidTxnVersion, false
	}

	tree := d.getTree(key)
	if tree == nil {
		return nil, InvalidTxnVersion, false
	}

	// index order is reversed,
	// find the closing txn that's less than the given txn
	item, ok := seekClosestTxn(tree, txn)
	if !ok {
		return nil, InvalidTxnVersion, false
	}

	return item.Value, item.Version(), item.Estimate
}

// ValidateIterator validates the iteration descriptor by replaying and compare the recorded reads.
func (d *MVData) ValidateIterator(desc IteratorDescriptor, txn TxnIndex) bool {
	it := NewMVIterator(desc.IteratorOptions, txn, d.Iter(), nil)
	defer it.Close()

	var i int
	for ; it.Valid(); it.Next() {
		if desc.Stop != nil {
			if BytesBeyond(it.Key(), desc.Stop, desc.Ascending) {
				break
			}
		}

		if i >= len(desc.Reads) {
			return false
		}

		read := desc.Reads[i]
		if read.Version != it.Version() || !bytes.Equal(read.Key, it.Key()) {
			return false
		}

		i++
	}

	// we read an estimate value, fail the validation.
	if it.ReadEstimateValue() {
		return false
	}

	return i == len(desc.Reads)
}

func (d *MVData) Snapshot() (snapshot []KVPair) {
	d.SnapshotTo(func(pair KVPair) bool {
		snapshot = append(snapshot, pair)
		return true
	})
	return
}

func (d *MVData) SnapshotTo(cb func(pair KVPair) bool) {
	d.Scan(func(outer dataItem) bool {
		// index order is reversed, `Min` is the latest
		item, ok := outer.Tree.Min()
		if !ok {
			return true
		}

		if item.Estimate {
			return true
		}

		return cb(KVPair{Key: outer.Key, Value: item.Value})
	})
}

type KVPair struct {
	Key   Key
	Value Value
}

type dataItem struct {
	Key  Key
	Tree *BTree[secondaryDataItem]
}

var _ KeyItem = dataItem{}

func (item dataItem) GetKey() []byte {
	return item.Key
}

type secondaryDataItem struct {
	Index       TxnIndex
	Incarnation Incarnation
	Value       []byte
	Estimate    bool
}

func secondaryLesser(a, b secondaryDataItem) bool {
	// reverse the order
	return a.Index > b.Index
}

func (item secondaryDataItem) Version() TxnVersion {
	return TxnVersion{Index: item.Index, Incarnation: item.Incarnation}
}

// seekClosestTxn returns the closest txn that's less than the given txn.
// NOTE: the tx index order is reversed.
func seekClosestTxn(tree *BTree[secondaryDataItem], txn TxnIndex) (secondaryDataItem, bool) {
	return tree.Seek(secondaryDataItem{Index: txn - 1})
}
