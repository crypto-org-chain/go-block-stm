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

func (bt *BTree[T]) Len() int {
	return bt.Load().Len()
}

func (bt *BTree[T]) Ascend(pivot T, iter func(item T) bool) {
	bt.Load().Ascend(pivot, iter)
}

func (bt *BTree[T]) Descend(pivot T, iter func(item T) bool) {
	bt.Load().Descend(pivot, iter)
}

func (bt *BTree[T]) Get(item T) (result T, ok bool) {
	return bt.Load().Get(item)
}

func (bt *BTree[T]) GetAt(index int) (result T, ok bool) {
	return bt.Load().GetAt(index)
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

func (bt *BTree[T]) DeleteAt(index int) (prev T, ok bool) {
	for {
		t := bt.Load()
		c := t.Copy()
		prev, ok = c.DeleteAt(index)
		c.Freeze()
		if bt.CompareAndSwap(t, c) {
			return
		}
	}
}

func (bt *BTree[T]) Height() int {
	return bt.Load().Height()
}

func (tr *BTree[T]) Walk(iter func(items []T)) {
	tr.Load().Walk(func(items []T) bool {
		iter(items)
		return true
	})
}

func (bt *BTree[T]) Copy() *BTree[T] {
	tree := bt.Load().Copy()
	tree.Freeze()
	t := &BTree[T]{}
	t.Store(tree)
	return t
}

// Clear will delete all items.
func (bt *BTree[T]) Clear() {
	for {
		t := bt.Load()
		c := t.Copy()
		c.Clear()
		c.Freeze()
		if bt.CompareAndSwap(t, c) {
			return
		}
	}
}

func (bt *BTree[T]) Scan(iter func(item T) bool) {
	bt.Load().Scan(iter)
}

func (bt *BTree[T]) Reverse(iter func(item T) bool) {
	bt.Load().Reverse(iter)
}

func (bt *BTree[T]) LoadItem(item T) (prev T, ok bool) {
	for {
		t := bt.Load()
		c := t.Copy()
		prev, ok = c.Load(item)
		c.Freeze()
		if bt.CompareAndSwap(t, c) {
			return
		}
	}
}

func (bt *BTree[T]) Min() (T, bool) {
	return bt.Load().Min()
}

func (bt *BTree[T]) Max() (T, bool) {
	return bt.Load().Max()
}

func (bt *BTree[T]) PopMin() (prev T, ok bool) {
	for {
		t := bt.Load()
		c := t.Copy()
		prev, ok = c.PopMin()
		c.Freeze()
		if bt.CompareAndSwap(t, c) {
			return
		}
	}
}

func (bt *BTree[T]) PopMax() (prev T, ok bool) {
	for {
		t := bt.Load()
		c := t.Copy()
		prev, ok = c.PopMax()
		c.Freeze()
		if bt.CompareAndSwap(t, c) {
			return
		}
	}
}

func (bt *BTree[T]) SetHint(item T, hint *btree.PathHint) (prev T, ok bool) {
	for {
		t := bt.Load()
		c := t.Copy()
		prev, ok = c.SetHint(item, hint)
		c.Freeze()
		if bt.CompareAndSwap(t, c) {
			return
		}
	}
}

func (bt *BTree[T]) Iter() btree.IterG[T] {
	return bt.Load().Iter()
}

func (bt *BTree[T]) IterMut() btree.IterG[T] {
	return bt.Load().IterMut()
}

func (bt *BTree[T]) Seek(item T) (result T, ok bool) {
	iter := bt.Iter()
	if !iter.Seek(item) {
		iter.Release()
		return
	}

	result = iter.Item()
	ok = true
	iter.Release()
	return
}
