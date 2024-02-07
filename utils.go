package block_stm

import (
	"cmp"
	"errors"
	"fmt"
	"sync/atomic"
)

var ErrNotFound = errors.New("not found")

type ErrReadError struct {
	BlockingTxn TxnIndex
}

func (e ErrReadError) Error() string {
	return fmt.Sprintf("read error: blocked by txn %d", e.BlockingTxn)
}

// StoreMin implements a compare-and-swap operation that stores the minimum of the current value and the given value.
func StoreMin(a *atomic.Uint64, b uint64) {
	for {
		old := a.Load()
		if old <= b {
			return
		}
		if a.CompareAndSwap(old, b) {
			return
		}
	}
}

func DecreaseAtomic(a *atomic.Uint64) {
	a.Add(^uint64(0))
}

func IncreaseAtomic(a *atomic.Uint64) {
	a.Add(1)
}

// callback arguments: (value, is_new)
func DiffOrderedList[T cmp.Ordered](old, new []T, callback func(T, bool) bool) {
	i, j := 0, 0
	for i < len(old) && j < len(new) {
		if old[i] < new[j] {
			if !callback(old[i], false) {
				return
			}
			i++
		} else if old[i] > new[j] {
			if !callback(new[j], true) {
				return
			}
			j++
		} else {
			i++
			j++
		}
	}
	for ; i < len(old); i++ {
		if !callback(old[i], false) {
			return
		}
	}
	for ; j < len(new); j++ {
		if !callback(new[j], true) {
			return
		}
	}
}
