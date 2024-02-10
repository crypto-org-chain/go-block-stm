package block_stm

import (
	"strconv"
	"testing"

	"github.com/test-go/testify/require"
)

func BenchmarkBlockSTM(b *testing.B) {
	storage := NewMemDB()
	testCases := []struct {
		name  string
		block []Tx
	}{
		{"random-10000/100", testBlock(10000, 100)},
		{"no-conflict-10000", noConflictBlock(10000)},
		{"worst-case-10000", worstCaseBlock(10000)},
	}
	for _, tc := range testCases {
		for _, worker := range []int{1, 5, 10, 15, 20} {
			b.Run(tc.name+"-worker-"+strconv.Itoa(worker), func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					require.NoError(b, ExecuteBlock(storage, tc.block, worker))
				}
			})
		}
	}
}
