package block_stm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	storetypes "cosmossdk.io/store/types"
	"github.com/test-go/testify/require"
)

func accountName(i int64) string {
	return fmt.Sprintf("account%05d", i)
}

func testBlock(size int, accounts int) *MockBlock {
	txs := make([]Tx, size)
	g := rand.New(rand.NewSource(0))
	for i := 0; i < size; i++ {
		sender := g.Int63n(int64(accounts))
		receiver := g.Int63n(int64(accounts))
		txs[i] = BankTransferTx(i, accountName(sender), accountName(receiver), 1)
	}
	return NewMockBlock(txs)
}

func iterateBlock(size int, accounts int) *MockBlock {
	txs := make([]Tx, size)
	g := rand.New(rand.NewSource(0))
	for i := 0; i < size; i++ {
		sender := g.Int63n(int64(accounts))
		receiver := g.Int63n(int64(accounts))
		txs[i] = IterateTx(i, accountName(sender), accountName(receiver), 1)
	}
	return NewMockBlock(txs)
}

func noConflictBlock(size int) *MockBlock {
	txs := make([]Tx, size)
	for i := 0; i < size; i++ {
		sender := accountName(int64(i))
		txs[i] = BankTransferTx(i, sender, sender, 1)
	}
	return NewMockBlock(txs)
}

func worstCaseBlock(size int) *MockBlock {
	txs := make([]Tx, size)
	for i := 0; i < size; i++ {
		// all transactions are from the same account
		sender := "account0"
		txs[i] = BankTransferTx(i, sender, sender, 1)
	}
	return NewMockBlock(txs)
}

func determisticBlock() *MockBlock {
	return NewMockBlock([]Tx{
		NoopTx(0, "account0"),
		NoopTx(1, "account1"),
		NoopTx(2, "account1"),
		NoopTx(3, "account1"),
		NoopTx(4, "account3"),
		NoopTx(5, "account1"),
		NoopTx(6, "account4"),
		NoopTx(7, "account5"),
		NoopTx(8, "account6"),
	})
}

func TestSTM(t *testing.T) {
	stores := []storetypes.StoreKey{StoreKeyAuth, StoreKeyBank}
	testCases := []struct {
		name      string
		blk       *MockBlock
		executors int
	}{
		{
			name:      "testBlock(100,80),10",
			blk:       testBlock(100, 80),
			executors: 10,
		},
		{
			name:      "testBlock(100,3),10",
			blk:       testBlock(100, 3),
			executors: 10,
		},
		{
			name:      "determisticBlock(),5",
			blk:       determisticBlock(),
			executors: 5,
		},
		{
			name:      "noConflictBlock(100),5",
			blk:       noConflictBlock(100),
			executors: 5,
		},
		{
			name:      "worstCaseBlock(100),5",
			blk:       worstCaseBlock(100),
			executors: 5,
		},
		{
			name:      "iterateBlock(100,80),10",
			blk:       iterateBlock(100, 80),
			executors: 10,
		},
		{
			name:      "iterateBlock(100,10),10",
			blk:       iterateBlock(100, 10),
			executors: 10,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			storage := NewMultiMemDB(stores)
			ExecuteBlock(tc.blk.Size(), stores, storage, tc.executors, tc.blk.Execute)
			for _, err := range tc.blk.Results {
				require.NoError(t, err)
			}

			crossCheck := NewMultiMemDB(stores)
			runSequential(crossCheck, tc.blk)

			// check parallel execution matches sequential execution
			for _, store := range stores {
				require.True(t, crossCheck.GetDB(store).Equal(storage.GetDB(store)))
			}

			// check total nonce increased the same amount as the number of transactions
			var total uint64
			storage.GetDB(StoreKeyAuth).Scan(func(k Key, v Value) bool {
				if !bytes.HasPrefix(k, []byte("nonce")) {
					return true
				}
				total += binary.BigEndian.Uint64(v)
				return true
			})
			require.Equal(t, uint64(tc.blk.Size()), total)
		})
	}
}
