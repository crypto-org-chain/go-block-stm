package block_stm

import (
	"bytes"

	"github.com/tidwall/btree"
)

type memdbItem struct {
	key   Key
	value Value
}

func memdbItemLess(a, b memdbItem) bool {
	return bytes.Compare(a.key, b.key) < 0
}

type MemDB struct {
	btree.BTreeG[memdbItem]
}

func NewMemDB() *MemDB {
	return &MemDB{*btree.NewBTreeG[memdbItem](memdbItemLess)}
}

// NewMemDBNonConcurrent returns a new BTree which is not safe for concurrent
// write operations by multiple goroutines.
func NewMemDBNonConcurrent() *MemDB {
	return &MemDB{*btree.NewBTreeGOptions[memdbItem](memdbItemLess, btree.Options{
		NoLocks: true,
	})}
}

func (db *MemDB) Scan(cb func(key Key, value Value) bool) {
	db.BTreeG.Scan(func(item memdbItem) bool {
		return cb(item.key, item.value)
	})
}

func (db *MemDB) Get(key Key) Value {
	item, ok := db.BTreeG.Get(memdbItem{key: key})
	if !ok {
		return nil
	}
	return item.value
}

func (db *MemDB) Set(key Key, value Value) {
	if value == nil {
		panic("nil value not allowed")
	}
	db.BTreeG.Set(memdbItem{key: key, value: value})
}
