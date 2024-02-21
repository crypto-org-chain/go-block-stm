package block_stm

import (
	"bytes"

	storetypes "cosmossdk.io/store/types"
	"github.com/tidwall/btree"
)

type memdbItem struct {
	key   Key
	value Value
}

var _ KeyItem = memdbItem{}

func (item memdbItem) GetKey() []byte {
	return item.key
}

type MemDB struct {
	btree.BTreeG[memdbItem]
}

var _ KVStore = (*MemDB)(nil)

func NewMemDB() *MemDB {
	return &MemDB{*btree.NewBTreeG[memdbItem](KeyItemLess)}
}

// NewMemDBNonConcurrent returns a new BTree which is not safe for concurrent
// write operations by multiple goroutines.
func NewMemDBNonConcurrent() *MemDB {
	return &MemDB{*btree.NewBTreeGOptions[memdbItem](KeyItemLess, btree.Options{
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

func (db *MemDB) Iterator(start, end Key) storetypes.Iterator {
	return db.iterator(start, end, true)
}

func (db *MemDB) ReverseIterator(start, end Key) storetypes.Iterator {
	return db.iterator(start, end, false)
}

func (db *MemDB) iterator(start, end Key, ascending bool) storetypes.Iterator {
	return NewMemDBIterator(start, end, db.Iter(), ascending)
}

func (db *MemDB) Equal(other *MemDB) bool {
	// compare with iterators
	iter1 := db.Iterator(nil, nil)
	iter2 := other.Iterator(nil, nil)
	defer iter1.Close()
	defer iter2.Close()

	for {
		if !iter1.Valid() && !iter2.Valid() {
			return true
		}
		if !iter1.Valid() || !iter2.Valid() {
			return false
		}
		if !bytes.Equal(iter1.Key(), iter2.Key()) || !bytes.Equal(iter1.Value(), iter2.Value()) {
			return false
		}
		iter1.Next()
		iter2.Next()
	}
}

type MemDBIterator struct {
	BTreeIteratorG[memdbItem]
}

var _ storetypes.Iterator = (*MemDBIterator)(nil)

func NewMemDBIterator(start, end Key, iter btree.IterG[memdbItem], ascending bool) *MemDBIterator {
	return &MemDBIterator{*NewBTreeIteratorG[memdbItem](
		memdbItem{key: start},
		memdbItem{key: end},
		iter,
		ascending,
	)}
}

func (it *MemDBIterator) Value() []byte {
	return it.Item().value
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
