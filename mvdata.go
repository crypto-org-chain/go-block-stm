package block_stm

import (
	"bytes"

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
			return nil, TxnVersion{}, nil
		}
	} else {
		if !iter.Last() {
			return nil, TxnVersion{}, nil
		}
	}

	item := iter.Item()

	if !bytes.Equal(item.Key, key) {
		return nil, TxnVersion{}, nil
	}
	if item.Estimate {
		return nil, TxnVersion{}, ErrReadError{BlockingTxn: item.Index}
	}
	return item.Value, item.Version(), nil
}

func (d *MVData) Snapshot() []KVPair {
	var snapshot []KVPair

	var lastPair KVPair
	d.inner.Scan(func(item dataItem) bool {
		if item.Estimate {
			return true
		}

		if lastPair.Key == nil {
			lastPair = KVPair{Key: item.Key, Value: item.Value}
			return true
		}

		if bytes.Equal(item.Key, lastPair.Key) {
			lastPair.Value = item.Value
			return true
		}

		snapshot = append(snapshot, lastPair)
		lastPair = KVPair{Key: item.Key, Value: item.Value}
		return true
	})

	snapshot = append(snapshot, lastPair)
	return snapshot
}

type KVPair struct {
	Key   Key
	Value Value
}

type dataItem struct {
	Key         Key
	Index       TxnIndex
	Incarnation Incarnation
	Value       []byte
	Estimate    bool
}

func dataItemLess(a, b dataItem) bool {
	switch bytes.Compare(a.Key, b.Key) {
	case -1:
		return true
	case 1:
		return false
	default:
		return a.Index < b.Index
	}
}

func (item dataItem) Version() TxnVersion {
	return TxnVersion{Index: item.Index, Incarnation: item.Incarnation}
}
