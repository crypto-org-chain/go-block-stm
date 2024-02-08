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

	wroteNewLocation := mv.Record(TxnVersion{1, 1}, ReadSet{
		ReadDescriptor{Key("a"), InvalidTxnVersion},
		ReadDescriptor{Key("d"), InvalidTxnVersion},
	}, BuildWriteSet(
		KVPair{Key("a"), Value("1")},
		KVPair{Key("b"), Value("1")},
		KVPair{Key("c"), Value("1")},
	))
	require.True(t, wroteNewLocation)
	require.True(t, mv.ValidateReadSet(1))

	value, version, err := mv.Read(Key("a"), 2)
	require.NoError(t, err)
	require.Equal(t, Value("1"), value)
	require.Equal(t, TxnVersion{1, 1}, version)

	wroteNewLocation = mv.Record(TxnVersion{1, 2}, nil, BuildWriteSet(
		KVPair{Key("a"), Value("2")},
		KVPair{Key("b"), Value("2")},
		KVPair{Key("c"), Value("2")},
	))
	require.False(t, wroteNewLocation)

	value, version, err = mv.Read(Key("a"), 2)
	require.NoError(t, err)
	require.Equal(t, Value("2"), value)
	require.Equal(t, TxnVersion{1, 2}, version)
}
