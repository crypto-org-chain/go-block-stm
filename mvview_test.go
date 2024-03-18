package block_stm

import (
	"fmt"
	"testing"

	storetypes "cosmossdk.io/store/types"
	"github.com/test-go/testify/require"
)

func TestMVMemoryViewDelete(t *testing.T) {
	stores := []storetypes.StoreKey{StoreKeyAuth}
	storesByName := IndexStores(stores)
	mv := NewMVMemory(16, stores)
	storage := NewMultiMemDB(stores)

	mview := NewMultiMVMemoryView(storesByName, storage, mv, nil, 0)
	view := mview.GetKVStore(StoreKeyAuth)
	view.Set(Key("a"), Value("1"))
	view.Set(Key("b"), Value("1"))
	view.Set(Key("c"), Value("1"))
	rs, ws := mview.Result()
	require.True(t, mv.Record(TxnVersion{0, 0}, rs, ws))

	mview = NewMultiMVMemoryView(storesByName, storage, mv, nil, 1)
	view = mview.GetKVStore(StoreKeyAuth)
	view.Delete(Key("a"))
	view.Set(Key("b"), Value("2"))
	rs, ws = mview.Result()
	require.True(t, mv.Record(TxnVersion{1, 0}, rs, ws))

	mview = NewMultiMVMemoryView(storesByName, storage, mv, nil, 2)
	view = mview.GetKVStore(StoreKeyAuth)
	require.Nil(t, view.Get(Key("a")))
	require.False(t, view.Has(Key("a")))
}

func TestMVMemoryViewIteration(t *testing.T) {
	stores := []storetypes.StoreKey{StoreKeyAuth}
	storesByName := IndexStores(stores)
	mv := NewMVMemory(16, stores)
	storage := NewMultiMemDB(stores)
	{
		parentState := []KVPair{
			{Key("a"), Value("1")},
			{Key("A"), Value("1")},
		}
		parent := storage.GetKVStore(StoreKeyAuth)
		for _, kv := range parentState {
			parent.Set(kv.Key, kv.Value)
		}
	}

	sets := [][]KVPair{
		{{Key("a"), Value("1")}, {Key("b"), Value("1")}, {Key("c"), Value("1")}},
		{{Key("b"), Value("2")}, {Key("c"), Value("2")}, {Key("d"), Value("2")}},
		{{Key("c"), Value("3")}, {Key("d"), Value("3")}, {Key("e"), Value("3")}},
		{{Key("d"), Value("4")}, {Key("f"), Value("4")}},
		{{Key("e"), Value("5")}, {Key("f"), Value("5")}, {Key("g"), Value("5")}},
		{{Key("f"), Value("6")}, {Key("g"), Value("6")}, {Key("a"), Value("6")}},
	}
	deletes := [][]Key{
		{},
		{},
		{Key("a")},
		{Key("A"), Key("e")},
		{},
		{Key("b"), Key("c"), Key("d")},
	}

	for i, pairs := range sets {
		mview := NewMultiMVMemoryView(storesByName, storage, mv, nil, TxnIndex(i))
		view := mview.GetKVStore(StoreKeyAuth)
		for _, kv := range pairs {
			view.Set(kv.Key, kv.Value)
		}
		for _, key := range deletes[i] {
			view.Delete(key)
		}
		rs, ws := mview.Result()
		require.True(t, mv.Record(TxnVersion{TxnIndex(i), 0}, rs, ws))
	}

	testCases := []struct {
		index      TxnIndex
		start, end Key
		ascending  bool
		expect     []KVPair
	}{
		{2, nil, nil, true, []KVPair{
			{Key("A"), Value("1")},
			{Key("a"), Value("1")},
			{Key("b"), Value("2")},
			{Key("c"), Value("2")},
			{Key("d"), Value("2")},
		}},
		{3, nil, nil, true, []KVPair{
			{Key("A"), Value("1")},
			{Key("b"), Value("2")},
			{Key("c"), Value("3")},
			{Key("d"), Value("3")},
			{Key("e"), Value("3")},
		}},
		{3, nil, nil, false, []KVPair{
			{Key("e"), Value("3")},
			{Key("d"), Value("3")},
			{Key("c"), Value("3")},
			{Key("b"), Value("2")},
			{Key("A"), Value("1")},
		}},
		{4, nil, nil, true, []KVPair{
			{Key("b"), Value("2")},
			{Key("c"), Value("3")},
			{Key("d"), Value("4")},
			{Key("f"), Value("4")},
		}},
		{5, nil, nil, true, []KVPair{
			{Key("b"), Value("2")},
			{Key("c"), Value("3")},
			{Key("d"), Value("4")},
			{Key("e"), Value("5")},
			{Key("f"), Value("5")},
			{Key("g"), Value("5")},
		}},
		{6, nil, nil, true, []KVPair{
			{Key("a"), Value("6")},
			{Key("e"), Value("5")},
			{Key("f"), Value("6")},
			{Key("g"), Value("6")},
		}},
		{6, Key("e"), Key("g"), true, []KVPair{
			{Key("e"), Value("5")},
			{Key("f"), Value("6")},
		}},
		{6, Key("e"), Key("g"), false, []KVPair{
			{Key("f"), Value("6")},
			{Key("e"), Value("5")},
		}},
		{6, Key("b"), nil, true, []KVPair{
			{Key("e"), Value("5")},
			{Key("f"), Value("6")},
			{Key("g"), Value("6")},
		}},
		{6, Key("b"), nil, false, []KVPair{
			{Key("g"), Value("6")},
			{Key("f"), Value("6")},
			{Key("e"), Value("5")},
		}},
		{6, nil, Key("g"), true, []KVPair{
			{Key("a"), Value("6")},
			{Key("e"), Value("5")},
			{Key("f"), Value("6")},
		}},
		{6, nil, Key("g"), false, []KVPair{
			{Key("f"), Value("6")},
			{Key("e"), Value("5")},
			{Key("a"), Value("6")},
		}},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("version-%d", tc.index), func(t *testing.T) {
			view := NewMultiMVMemoryView(storesByName, storage, mv, nil, tc.index).GetKVStore(StoreKeyAuth)
			var iter storetypes.Iterator
			if tc.ascending {
				iter = view.Iterator(tc.start, tc.end)
			} else {
				iter = view.ReverseIterator(tc.start, tc.end)
			}
			require.Equal(t, tc.expect, CollectIterator(iter))
			require.NoError(t, iter.Close())
		})
	}
}

func CollectIterator(iter storetypes.Iterator) []KVPair {
	var res []KVPair
	for iter.Valid() {
		res = append(res, KVPair{iter.Key(), iter.Value()})
		iter.Next()
	}
	return res
}
