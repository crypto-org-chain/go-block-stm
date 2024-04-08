package block_stm

import (
	"testing"

	storetypes "cosmossdk.io/store/types"
	"github.com/test-go/testify/require"
)

func TestMVMemoryRecord(t *testing.T) {
	stores := map[storetypes.StoreKey]int{StoreKeyAuth: 0}
	storage := NewMultiMemDB(stores)
	mv := NewMVMemory(16, stores)
	scheduler := NewScheduler(16)

	var views []*MultiMVMemoryView
	for i := TxnIndex(0); i < 3; i++ {
		version := TxnVersion{i, 0}
		view := mv.View(version.Index, storage, scheduler)
		store := view.GetKVStore(StoreKeyAuth)

		_ = store.Get([]byte("a"))
		_ = store.Get([]byte("d"))
		store.Set([]byte("a"), []byte("1"))
		store.Set([]byte("b"), []byte("1"))
		store.Set([]byte("c"), []byte("1"))

		views = append(views, view)
	}

	for i, view := range views {
		wroteNewLocation := mv.Record(TxnVersion{TxnIndex(i), 0}, view)
		require.True(t, wroteNewLocation)
	}

	require.True(t, mv.ValidateReadSet(0))
	require.False(t, mv.ValidateReadSet(1))
	require.False(t, mv.ValidateReadSet(2))

	// abort 2 and 3
	mv.ConvertWritesToEstimates(1)
	mv.ConvertWritesToEstimates(2)

	resultCh := make(chan struct{}, 1)
	go func() {
		view := mv.View(3, storage, scheduler)
		store := view.GetKVStore(StoreKeyAuth)
		// will wait for tx 2
		store.Get([]byte("a"))
		wroteNewLocation := mv.Record(TxnVersion{3, 1}, view)
		require.False(t, wroteNewLocation)
		require.True(t, mv.ValidateReadSet(3))
		resultCh <- struct{}{}
	}()

	{
		data := mv.GetMVStore(0).(*MVData)
		value, version, estimate := data.Read(Key("a"), 1)
		require.False(t, estimate)
		require.Equal(t, []byte("1"), value)
		require.Equal(t, TxnVersion{0, 0}, version)

		_, version, estimate = data.Read(Key("a"), 2)
		require.True(t, estimate)
		require.Equal(t, TxnIndex(1), version.Index)

		_, version, estimate = data.Read(Key("a"), 3)
		require.True(t, estimate)
		require.Equal(t, TxnIndex(2), version.Index)
	}

	// rerun tx 1
	{
		view := mv.View(1, storage, scheduler)
		store := view.GetKVStore(StoreKeyAuth)

		_ = store.Get([]byte("a"))
		_ = store.Get([]byte("d"))
		store.Set([]byte("a"), []byte("2"))
		store.Set([]byte("b"), []byte("2"))
		store.Set([]byte("c"), []byte("2"))

		wroteNewLocation := mv.Record(TxnVersion{1, 1}, view)
		require.False(t, wroteNewLocation)
		require.True(t, mv.ValidateReadSet(1))
	}

	// rerun tx 2
	// don't write `c` this time
	{
		version := TxnVersion{2, 1}
		view := mv.View(version.Index, storage, scheduler)
		store := view.GetKVStore(StoreKeyAuth)

		_ = store.Get([]byte("a"))
		_ = store.Get([]byte("d"))
		store.Set([]byte("a"), []byte("3"))
		store.Set([]byte("b"), []byte("3"))

		wroteNewLocation := mv.Record(version, view)
		require.False(t, wroteNewLocation)
		require.True(t, mv.ValidateReadSet(2))

		scheduler.FinishExecution(version, wroteNewLocation)

		// wait for dependency to finish
		<-resultCh
	}

	// run tx 3
	{
		view := mv.View(3, storage, scheduler)
		store := view.GetKVStore(StoreKeyAuth)

		_ = store.Get([]byte("a"))

		wroteNewLocation := mv.Record(TxnVersion{3, 1}, view)
		require.False(t, wroteNewLocation)
		require.True(t, mv.ValidateReadSet(3))
	}

	{
		data := mv.GetMVStore(0).(*MVData)
		value, version, estimate := data.Read(Key("a"), 2)
		require.False(t, estimate)
		require.Equal(t, []byte("2"), value)
		require.Equal(t, TxnVersion{1, 1}, version)

		value, version, estimate = data.Read(Key("a"), 3)
		require.False(t, estimate)
		require.Equal(t, []byte("3"), value)
		require.Equal(t, TxnVersion{2, 1}, version)

		value, version, estimate = data.Read(Key("c"), 3)
		require.False(t, estimate)
		require.Equal(t, []byte("2"), value)
		require.Equal(t, TxnVersion{1, 1}, version)
	}
}
