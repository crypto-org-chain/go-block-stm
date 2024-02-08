package block_stm

import (
	"testing"

	"github.com/test-go/testify/require"
)

func BuildWriteSet(pairs ...KVPair) WriteSet {
	ws := NewWriteSet()
	for _, pair := range pairs {
		ws.MustSet(pair.Key, pair.Value)
	}
	return ws
}

func TestMVMemoryRecord(t *testing.T) {
	mv := NewMVMemory(16)

	for i := TxnIndex(0); i < 3; i++ {
		wroteNewLocation := mv.Record(TxnVersion{i, 0}, ReadSet{
			ReadDescriptor{Key("a"), InvalidTxnVersion},
			ReadDescriptor{Key("d"), InvalidTxnVersion},
		}, BuildWriteSet(
			KVPair{Key("a"), Value("1")},
			KVPair{Key("b"), Value("1")},
			KVPair{Key("c"), Value("1")},
		))
		require.True(t, wroteNewLocation)
	}
	require.True(t, mv.ValidateReadSet(0))
	require.False(t, mv.ValidateReadSet(1))
	require.False(t, mv.ValidateReadSet(2))

	// abort 2 and 3
	mv.ConvertWritesToEstimates(1)
	mv.ConvertWritesToEstimates(2)

	wroteNewLocation := mv.Record(TxnVersion{3, 1}, ReadSet{
		// simulate a read of a key that's ESTIMATE
		ReadDescriptor{Key("a"), TxnVersion{2, 1}},
	}, BuildWriteSet())
	require.False(t, wroteNewLocation)
	require.False(t, mv.ValidateReadSet(3))

	value, version, err := mv.Read(Key("a"), 1)
	require.NoError(t, err)
	require.Equal(t, Value("1"), value)
	require.Equal(t, TxnVersion{0, 0}, version)

	_, _, err = mv.Read(Key("a"), 2)
	require.Error(t, err)
	require.Equal(t, TxnIndex(1), err.(ErrReadError).BlockingTxn)

	_, _, err = mv.Read(Key("a"), 3)
	require.Error(t, err)
	require.Equal(t, TxnIndex(2), err.(ErrReadError).BlockingTxn)

	// rerun tx 1
	wroteNewLocation = mv.Record(TxnVersion{1, 1}, ReadSet{
		ReadDescriptor{Key("a"), TxnVersion{0, 0}},
		ReadDescriptor{Key("d"), InvalidTxnVersion},
	}, BuildWriteSet(
		KVPair{Key("a"), Value("2")},
		KVPair{Key("b"), Value("2")},
		KVPair{Key("c"), Value("2")},
	))
	require.False(t, wroteNewLocation)
	require.True(t, mv.ValidateReadSet(1))

	// rerun tx 2
	// don't write `c` this time
	wroteNewLocation = mv.Record(TxnVersion{2, 1}, ReadSet{
		ReadDescriptor{Key("a"), TxnVersion{1, 1}},
		ReadDescriptor{Key("d"), InvalidTxnVersion},
	}, BuildWriteSet(
		KVPair{Key("a"), Value("3")},
		KVPair{Key("b"), Value("3")},
	))
	require.False(t, wroteNewLocation)
	require.True(t, mv.ValidateReadSet(2))

	// run tx 3
	wroteNewLocation = mv.Record(TxnVersion{3, 1}, ReadSet{
		// simulate a read of a key that's deleted later.
		ReadDescriptor{Key("d"), TxnVersion{1, 1}},
	}, BuildWriteSet())
	require.False(t, wroteNewLocation)
	require.False(t, mv.ValidateReadSet(3))

	value, version, err = mv.Read(Key("a"), 2)
	require.NoError(t, err)
	require.Equal(t, Value("2"), value)
	require.Equal(t, TxnVersion{1, 1}, version)

	value, version, err = mv.Read(Key("a"), 3)
	require.NoError(t, err)
	require.Equal(t, Value("3"), value)
	require.Equal(t, TxnVersion{2, 1}, version)

	value, version, err = mv.Read(Key("c"), 3)
	require.NoError(t, err)
	require.Equal(t, Value("2"), value)
	require.Equal(t, TxnVersion{1, 1}, version)
}
