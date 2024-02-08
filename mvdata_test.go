package block_stm

import (
	"errors"
	"fmt"
	"testing"

	"github.com/test-go/testify/require"
)

func TestEmptyMVData(t *testing.T) {
	data := NewMVData()
	value, _, err := data.Read([]byte("a"), 1)
	require.NoError(t, err)
	require.Nil(t, value)
}

func TestMVData(t *testing.T) {
	data := NewMVData()

	// read closest version
	data.Write([]byte("a"), []byte("1"), TxnVersion{Index: 1, Incarnation: 1}, nil)
	data.Write([]byte("a"), []byte("2"), TxnVersion{Index: 2, Incarnation: 1}, nil)
	data.Write([]byte("a"), []byte("3"), TxnVersion{Index: 3, Incarnation: 1}, nil)
	data.Write([]byte("b"), []byte("2"), TxnVersion{Index: 2, Incarnation: 1}, nil)

	// read closest version
	value, _, err := data.Read([]byte("a"), 1)
	require.NoError(t, err)
	require.Nil(t, value)

	// read closest version
	value, version, err := data.Read([]byte("a"), 4)
	require.NoError(t, err)
	require.Equal(t, Value([]byte("3")), value)
	require.Equal(t, TxnVersion{Index: 3, Incarnation: 1}, version)

	// read closest version
	value, version, err = data.Read([]byte("a"), 3)
	require.NoError(t, err)
	require.Equal(t, Value([]byte("2")), value)
	require.Equal(t, TxnVersion{Index: 2, Incarnation: 1}, version)

	// read closest version
	value, version, err = data.Read([]byte("b"), 3)
	require.NoError(t, err)
	require.Equal(t, Value([]byte("2")), value)
	require.Equal(t, TxnVersion{Index: 2, Incarnation: 1}, version)

	// new incarnation overrides old
	data.Write([]byte("a"), []byte("3-2"), TxnVersion{Index: 3, Incarnation: 2}, nil)
	value, version, err = data.Read([]byte("a"), 4)
	require.NoError(t, err)
	require.Equal(t, Value([]byte("3-2")), value)
	require.Equal(t, TxnVersion{Index: 3, Incarnation: 2}, version)

	// read estimate
	data.WriteEstimate([]byte("a"), 3, nil)
	_, _, err = data.Read([]byte("a"), 4)
	require.Error(t, err)
	require.Equal(t, TxnIndex(3), err.(ErrReadError).BlockingTxn)

	// delete value
	data.Delete([]byte("a"), 3, nil)
	value, version, err = data.Read([]byte("a"), 4)
	require.NoError(t, err)
	require.Equal(t, Value([]byte("2")), value)
	require.Equal(t, TxnVersion{Index: 2, Incarnation: 1}, version)

	data.Delete([]byte("b"), 2, nil)
	value, _, err = data.Read([]byte("b"), 4)
	require.NoError(t, err)
	require.Nil(t, value)
}

func TestReadErrConversion(t *testing.T) {
	err := fmt.Errorf("wrap: %w", ErrReadError{BlockingTxn: 1})
	var readErr ErrReadError
	require.True(t, errors.As(err, &readErr))
	require.Equal(t, TxnIndex(1), readErr.BlockingTxn)
}

func TestSnapshot(t *testing.T) {
	data := NewMVData()
	// read closest version
	data.Write([]byte("a"), []byte("1"), TxnVersion{Index: 1, Incarnation: 1}, nil)
	data.Write([]byte("a"), []byte("2"), TxnVersion{Index: 2, Incarnation: 1}, nil)
	data.Write([]byte("a"), []byte("3"), TxnVersion{Index: 3, Incarnation: 1}, nil)
	data.Write([]byte("b"), []byte("2"), TxnVersion{Index: 2, Incarnation: 1}, nil)
	data.WriteEstimate([]byte("c"), 2, nil)

	require.Equal(t, []KVPair{
		{[]byte("a"), []byte("3")},
		{[]byte("b"), []byte("2")},
	}, data.Snapshot())
}
