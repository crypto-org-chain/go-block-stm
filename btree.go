package block_stm

import (
	"sync/atomic"

	"github.com/tidwall/btree"
)

// BTree wraps an atomic pointer to an unsafe btree.BTreeG
type BTree[T any] struct {
	atomic.Pointer[btree.BTreeG[T]]
}

// NewBTree returns a new BTree.
func NewBTree[T any](less func(a, b T) bool) *BTree[T] {
	tree := btree.NewBTreeGOptions[T](less, btree.Options{
		NoLocks:  true,
		ReadOnly: true,
	})
	t := &BTree[T]{}
	t.Store(tree)
	return t
}

func (bt *BTree[T]) Get(item T) (result T, ok bool) {
	return bt.Load().Get(item)
}

func (bt *BTree[T]) GetOrDefault(item T, fillDefaults func(*T)) T {
	for {
		t := bt.Load()
		result, ok := t.Get(item)
		if ok {
			return result
		}
		fillDefaults(&item)
		c := t.Copy()
		c.Set(item)
		c.Freeze()
		if bt.CompareAndSwap(t, c) {
			return item
		}
	}
}

func (bt *BTree[T]) Set(item T) (prev T, ok bool) {
	for {
		t := bt.Load()
		c := t.Copy()
		prev, ok = c.Set(item)
		c.Freeze()
		if bt.CompareAndSwap(t, c) {
			return
		}
	}
}

func (bt *BTree[T]) Delete(item T) (prev T, ok bool) {
	for {
		t := bt.Load()
		c := t.Copy()
		prev, ok = c.Delete(item)
		c.Freeze()
		if bt.CompareAndSwap(t, c) {
			return
		}
	}
}

func (bt *BTree[T]) Scan(iter func(item T) bool) {
	bt.Load().Scan(iter)
}

func (bt *BTree[T]) Max() (item T, ok bool) {
	return bt.Load().Max()
}

func (bt *BTree[T]) Min() (item T, ok bool) {
	return bt.Load().Min()
}

func (bt *BTree[T]) Seek(item T) (T, bool) {
	var empty T
	iter := bt.Load().Iter()
	if !iter.Seek(item) {
		return empty, false
	}

	return iter.Item(), true
}
