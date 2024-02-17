package block_stm

import (
	"testing"

	"github.com/test-go/testify/require"
)

func BuildWriteSet(pairs ...KVPair) WriteSet {
	ws := NewWriteSet()
	for _, pair := range pairs {
		ws.Set(pair.Key, pair.Value)
	}
	return ws
}

func TestMVMemoryRecord(t *testing.T) {
	stores := []string{"acc"}
	mv := NewMVMemory(16, stores)

	for i := TxnIndex(0); i < 3; i++ {
		wroteNewLocation := mv.Record(TxnVersion{i, 0}, MultiReadSet{{
			ReadDescriptor{Key("a"), InvalidTxnVersion},
			ReadDescriptor{Key("d"), InvalidTxnVersion},
		}}, MultiWriteSet{BuildWriteSet(
			KVPair{Key("a"), Value("1")},
			KVPair{Key("b"), Value("1")},
			KVPair{Key("c"), Value("1")},
		)})
		require.True(t, wroteNewLocation)
	}
	require.True(t, mv.ValidateReadSet(0))
	require.False(t, mv.ValidateReadSet(1))
	require.False(t, mv.ValidateReadSet(2))

	// abort 2 and 3
	mv.ConvertWritesToEstimates(1)
	mv.ConvertWritesToEstimates(2)

	wroteNewLocation := mv.Record(TxnVersion{3, 1}, MultiReadSet{ReadSet{
		// simulate a read of a key that's ESTIMATE
		ReadDescriptor{Key("a"), TxnVersion{2, 1}},
	}}, MultiWriteSet{BuildWriteSet()})
	require.False(t, wroteNewLocation)
	require.False(t, mv.ValidateReadSet(3))

	value, version, estimate := mv.Read(0, Key("a"), 1)
	require.False(t, estimate)
	require.Equal(t, Value("1"), value)
	require.Equal(t, TxnVersion{0, 0}, version)

	_, version, estimate = mv.Read(0, Key("a"), 2)
	require.True(t, estimate)
	require.Equal(t, TxnIndex(1), version.Index)

	_, version, estimate = mv.Read(0, Key("a"), 3)
	require.True(t, estimate)
	require.Equal(t, TxnIndex(2), version.Index)

	// rerun tx 1
	wroteNewLocation = mv.Record(TxnVersion{1, 1}, MultiReadSet{ReadSet{
		ReadDescriptor{Key("a"), TxnVersion{0, 0}},
		ReadDescriptor{Key("d"), InvalidTxnVersion},
	}}, MultiWriteSet{BuildWriteSet(
		KVPair{Key("a"), Value("2")},
		KVPair{Key("b"), Value("2")},
		KVPair{Key("c"), Value("2")},
	)})
	require.False(t, wroteNewLocation)
	require.True(t, mv.ValidateReadSet(1))

	// rerun tx 2
	// don't write `c` this time
	wroteNewLocation = mv.Record(TxnVersion{2, 1}, MultiReadSet{ReadSet{
		ReadDescriptor{Key("a"), TxnVersion{1, 1}},
		ReadDescriptor{Key("d"), InvalidTxnVersion},
	}}, MultiWriteSet{BuildWriteSet(
		KVPair{Key("a"), Value("3")},
		KVPair{Key("b"), Value("3")},
	)})
	require.False(t, wroteNewLocation)
	require.True(t, mv.ValidateReadSet(2))

	// run tx 3
	wroteNewLocation = mv.Record(TxnVersion{3, 1}, MultiReadSet{ReadSet{
		// simulate a read of a key that's deleted later.
		ReadDescriptor{Key("d"), TxnVersion{1, 1}},
	}}, MultiWriteSet{BuildWriteSet()})
	require.False(t, wroteNewLocation)
	require.False(t, mv.ValidateReadSet(3))

	value, version, estimate = mv.Read(0, Key("a"), 2)
	require.False(t, estimate)
	require.Equal(t, Value("2"), value)
	require.Equal(t, TxnVersion{1, 1}, version)

	value, version, estimate = mv.Read(0, Key("a"), 3)
	require.False(t, estimate)
	require.Equal(t, Value("3"), value)
	require.Equal(t, TxnVersion{2, 1}, version)

	value, version, estimate = mv.Read(0, Key("c"), 3)
	require.False(t, estimate)
	require.Equal(t, Value("2"), value)
	require.Equal(t, TxnVersion{1, 1}, version)
}

func TestMVMemoryDelete(t *testing.T) {
	stores := []string{"acc"}
	mv := NewMVMemory(16, stores)
	storage := NewMultiMemDB(stores)

	mview := NewMultiMVMemoryView(stores, storage, mv, nil, 0)
	view := mview.GetKVStore("acc")
	view.Set(Key("a"), Value("1"))
	view.Set(Key("b"), Value("1"))
	view.Set(Key("c"), Value("1"))
	rs, ws := mview.Result()
	require.True(t, mv.Record(TxnVersion{0, 0}, rs, ws))

	mview = NewMultiMVMemoryView(stores, storage, mv, nil, 1)
	view = mview.GetKVStore("acc")
	view.Delete(Key("a"))
	view.Set(Key("b"), Value("2"))
	rs, ws = mview.Result()
	require.True(t, mv.Record(TxnVersion{1, 0}, rs, ws))

	mview = NewMultiMVMemoryView(stores, storage, mv, nil, 2)
	view = mview.GetKVStore("acc")
	require.Nil(t, view.Get(Key("a")))
	require.False(t, view.Has(Key("a")))
}
