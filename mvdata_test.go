package block_stm

import (
	"testing"

	"github.com/test-go/testify/require"
)

func TestMVData(t *testing.T) {
	data := NewMVData()

	// read closest version
	data.Write("a", []byte("1"), TxnVersion{Index: 1, Incarnation: 1}, nil)
	data.Write("a", []byte("2"), TxnVersion{Index: 2, Incarnation: 1}, nil)
	data.Write("a", []byte("3"), TxnVersion{Index: 3, Incarnation: 1}, nil)
	data.Write("b", []byte("3"), TxnVersion{Index: 2, Incarnation: 1}, nil)

	value, version, err := data.Read("a", 4)
	require.NoError(t, err)
	require.Equal(t, Value([]byte("3")), value)
	require.Equal(t, TxnVersion{Index: 3, Incarnation: 1}, version)
}
