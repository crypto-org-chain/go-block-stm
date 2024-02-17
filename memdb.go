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

var _ KVStore = (*MemDB)(nil)

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

func (db *MemDB) Has(key Key) bool {
	return db.Get(key) != nil
}

func (db *MemDB) Set(key Key, value Value) {
	if value == nil {
		panic("nil value not allowed")
	}
	db.BTreeG.Set(memdbItem{key: key, value: value})
}

func (db *MemDB) Delete(key Key) {
	db.BTreeG.Delete(memdbItem{key: key})
}

// When used as an overlay (e.g. WriteSet), it stores the `nil` value to represent deleted keys,
// so we return seperate bool value for found status.
func (db *MemDB) OverlayGet(key Key) (Value, bool) {
	item, ok := db.BTreeG.Get(memdbItem{key: key})
	if !ok {
		return nil, false
	}
	return item.value, true
}

// When used as an overlay (e.g. WriteSet), it stores the `nil` value to represent deleted keys,
func (db *MemDB) OverlaySet(key Key, value Value) {
	db.BTreeG.Set(memdbItem{key: key, value: value})
}

type MultiMemDB struct {
	dbs map[string]*MemDB
}

func NewMultiMemDB(stores []string) *MultiMemDB {
	dbs := make(map[string]*MemDB, len(stores))
	for _, name := range stores {
		dbs[name] = NewMemDB()
	}
	return &MultiMemDB{
		dbs: dbs,
	}
}

func (mmdb *MultiMemDB) GetDB(store string) *MemDB {
	return mmdb.dbs[store]
}

func (mmdb *MultiMemDB) GetKVStore(store string) KVStore {
	return mmdb.GetDB(store)
}
