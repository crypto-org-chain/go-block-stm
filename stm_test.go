package block_stm

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"strconv"
	"testing"

	"github.com/test-go/testify/require"
)

func accountName(i int64) string {
	return "account" + strconv.FormatInt(i, 10)
}

func testBlock(size int, accounts int) []Tx {
	blk := make([]Tx, size)
	g := rand.New(rand.NewSource(0))
	for i := 0; i < size; i++ {
		sender := g.Int63n(int64(accounts))
		receiver := g.Int63n(int64(accounts))
		blk[i] = BankTransferTx(accountName(sender), accountName(receiver), 1)
	}
	return blk
}

func noConflictBlock(size int) []Tx {
	blk := make([]Tx, size)
	for i := 0; i < size; i++ {
		sender := accountName(int64(i))
		blk[i] = BankTransferTx(sender, sender, 1)
	}
	return blk
}

func worstCaseBlock(size int) []Tx {
	blk := make([]Tx, size)
	for i := 0; i < size; i++ {
		// all transactions are from the same account
		sender := "account0"
		blk[i] = BankTransferTx(sender, sender, 1)
	}
	return blk
}

func determisticBlock() []Tx {
	return []Tx{
		NoopTx("account0"),
		NoopTx("account1"),
		NoopTx("account1"),
		NoopTx("account1"),
		NoopTx("account3"),
		NoopTx("account1"),
		NoopTx("account4"),
		NoopTx("account5"),
		NoopTx("account6"),
	}
}

func TestSTM(t *testing.T) {
	testCases := []struct {
		name      string
		blk       []Tx
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
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			storage := NewMemDB()
			require.NoError(t, ExecuteBlock(storage, tc.blk, tc.executors))

			// check total nonce increased the same amount as the number of transactions
			var total uint64
			storage.Scan(func(k Key, v Value) bool {
				if !bytes.HasPrefix(k, []byte("nonce")) {
					return true
				}
				total += binary.BigEndian.Uint64(v)
				return true
			})
			require.Equal(t, uint64(len(tc.blk)), total)
		})
	}
}
