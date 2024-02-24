package block_stm

import (
	"strconv"
	"testing"

	storetypes "cosmossdk.io/store/types"
)

func BenchmarkBlockSTM(b *testing.B) {
	stores := []storetypes.StoreKey{StoreKeyAuth, StoreKeyBank}
	storage := NewMultiMemDB(stores)
	testCases := []struct {
		name  string
		block *MockBlock
	}{
		{"random-10000/100", testBlock(10000, 100)},
		{"no-conflict-10000", noConflictBlock(10000)},
		{"worst-case-10000", worstCaseBlock(10000)},
		{"iterate-10000/100", iterateBlock(10000, 100)},
	}
	for _, tc := range testCases {
		b.Run(tc.name+"-sequential", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runSequential(storage, tc.block)
			}
		})
		for _, worker := range []int{1, 5, 10, 15, 20} {
			b.Run(tc.name+"-worker-"+strconv.Itoa(worker), func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					ExecuteBlock(tc.block.Size(), stores, storage, worker, tc.block.Execute)
				}
			})
		}
	}
}

func runSequential(storage MultiStore, block *MockBlock) {
	for i, tx := range block.Txs {
		block.Results[i] = tx(storage)
	}
}
