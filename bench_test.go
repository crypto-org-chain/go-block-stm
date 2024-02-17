package block_stm

import (
	"strconv"
	"testing"

	"github.com/test-go/testify/require"
)

func BenchmarkBlockSTM(b *testing.B) {
	stores := []string{"acc", "bank"}
	storage := NewMultiMemDB(stores)
	testCases := []struct {
		name  string
		block []Tx
	}{
		{"random-10000/100", testBlock(10000, 100)},
		{"no-conflict-10000", noConflictBlock(10000)},
		{"worst-case-10000", worstCaseBlock(10000)},
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
					require.NoError(b, ExecuteBlock(stores, storage, tc.block, worker))
				}
			})
		}
	}
}

func runSequential(storage MultiStore, block []Tx) {
	for _, tx := range block {
		tx(storage)
	}
}
