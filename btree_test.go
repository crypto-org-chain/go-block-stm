package block_stm

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/tidwall/btree"
)

func assert(x bool) {
	if !x {
		panic("assert failed")
	}
}

func intLess(a, b int) bool {
	return a < b
}

func TestBTree(t *testing.T) {
	N := 1_000_000
	for j := 0; j < 2; j++ {
		tr := NewBTree(intLess)
		for i := 0; i < N; i++ {
			_, ok := tr.LoadItem(i)
			assert(!ok)
		}
		assert(tr.Len() == N)
		for i := 0; i < N; i++ {
			result, _ := tr.Get(i)
			assert(result == i)
		}

		count := 0
		tr.Scan(func(_ int) bool {
			count++
			return true
		})
		assert(count == N)
		count = 0
		tr.Ascend(N/2, func(_ int) bool {
			count++
			return true
		})
		assert(count == N/2)

		count = 0
		tr.Reverse(func(_ int) bool {
			count++
			return true
		})
		assert(count == N)
		count = 0
		tr.Descend(N/2, func(_ int) bool {
			count++
			return true
		})
		assert(count == N/2+1)

		for i := 0; i < N; i++ {
			result, _ := tr.Delete(i)
			assert(result == i)
		}
		assert(tr.Len() == 0)
		_, ok := tr.Min()
		assert(!ok)
		_, ok = tr.Max()
		assert(!ok)
		_, ok = tr.PopMin()
		assert(!ok)
		_, ok = tr.PopMax()
		assert(!ok)

		for i := 0; i < N; i++ {
			_, ok = tr.Get(i)
			assert(!ok)
		}
		for i := 0; i < N; i++ {
			_, ok = tr.Set(i)
			assert(!ok)
		}
		assert(tr.Len() == N)
		var hint btree.PathHint
		for i := 0; i < N; i++ {
			result, _ := tr.SetHint(i, &hint)
			assert(result == i)
		}
		assert(tr.Len() == N)
		for i := 0; i < N; i++ {
			result, _ := tr.LoadItem(i)
			assert(result == i)
		}
		assert(tr.Len() == N)
		result, _ := tr.Min()
		assert(result == 0)
		result, _ = tr.Max()
		assert(result == N-1)
		result, _ = tr.PopMin()
		assert(result == 0)
		result, _ = tr.PopMax()
		assert(result == N-1)
		_, ok = tr.Set(0)
		assert(!ok)
		_, ok = tr.Set(N - 1)
		assert(!ok)
		result, _ = tr.GetAt(0)
		assert(result == 0)
		_, ok = tr.GetAt(N)
		assert(!ok)
		result, _ = tr.Set(N - 1)
		assert(result == N-1)
		assert(tr.Height() > 0)
		result, _ = tr.DeleteAt(0)
		assert(result == 0)
		_, ok = tr.Set(0)
		assert(!ok)
		result, _ = tr.DeleteAt(N - 1)
		assert(result == N-1)
		_, ok = tr.DeleteAt(N)
		assert(!ok)
		var wg sync.WaitGroup
		wg.Add(1)
		go func(tr *BTree[int]) {
			wg.Wait()
			count := 0
			tr.Walk(func(items []int) {
				count += len(items)
			})
			assert(count == N-1)
		}(tr.Copy())
		for i := 0; i < N/2; i++ {
			tr.Delete(i)
		}
		for i := 0; i < N; i++ {
			tr.Set(i)
		}
		wg.Done()

		count = 0
		tr.Walk(func(items []int) {
			count += len(items)
		})
		assert(count == N)
	}
}

func TestClear(t *testing.T) {
	tr := NewBTree(intLess)
	for i := 0; i < 100; i++ {
		tr.Set(i)
	}
	assert(tr.Len() == 100)
	tr.Clear()
	assert(tr.Len() == 0)
	for i := 0; i < 100; i++ {
		tr.Set(i)
	}
	assert(tr.Len() == 100)
}

func TestIter(t *testing.T) {
	N := 100_000
	lt := func(a, b interface{}) bool { return a.(int) < b.(int) }
	eq := func(a, b interface{}) bool { return !lt(a, b) && !lt(b, a) }
	tr := NewBTree(lt)
	var all []int
	for i := 0; i < N; i++ {
		tr.LoadItem(i)
		all = append(all, i)
	}
	var count int
	var i int
	iter := tr.Iter()
	for ok := iter.First(); ok; ok = iter.Next() {
		if !eq(all[i], iter.Item()) {
			panic("!")
		}
		count++
		i++
	}
	if count != N {
		t.Fatalf("expected %v, got %v", N, count)
	}
	iter.Release()
	count = 0
	i = len(all) - 1
	iter = tr.Iter()
	for ok := iter.Last(); ok; ok = iter.Prev() {
		if !eq(all[i], iter.Item()) {
			panic("!")
		}
		i--
		count++
	}
	if count != N {
		t.Fatalf("expected %v, got %v", N, count)
	}
	iter.Release()
	i = 0
	iter = tr.Iter()
	for ok := iter.First(); ok; ok = iter.Next() {
		if !eq(all[i], iter.Item()) {
			panic("!")
		}
		i++
	}
	i--
	for ok := iter.Prev(); ok; ok = iter.Prev() {
		i--
		if !eq(all[i], iter.Item()) {
			panic("!")
		}

	}
	if i != 0 {
		panic("!")
	}

	i++
	for ok := iter.Next(); ok; ok = iter.Next() {
		if !eq(all[i], iter.Item()) {
			panic("!")
		}
		i++

	}
	if i != N {
		panic("!")
	}

	i = 0
	for ok := iter.First(); ok; ok = iter.Next() {
		if !eq(all[i], iter.Item()) {
			panic("!")
		}
		if eq(iter.Item(), N/2) {
			for ok = iter.Prev(); ok; ok = iter.Prev() {
				i--
				if !eq(all[i], iter.Item()) {
					panic("!")
				}
			}
			break
		}
		i++
	}
	iter.Release()
}

func TestSeek(t *testing.T) {
	N := 100_000
	lt := func(a, b interface{}) bool { return a.(int) < b.(int) }
	eq := func(a, b interface{}) bool { return !lt(a, b) && !lt(b, a) }
	tr := NewBTree(lt)
	var all []int
	for i := 0; i < N; i++ {
		tr.LoadItem(i)
		all = append(all, i)
	}
	// test found
	for _, item := range all {
		result, ok := tr.Seek(item)
		if !ok || !eq(result, item) {
			panic("!")
		}
	}

	// test not found
	_, ok := tr.Seek(N + 1)
	if ok {
		panic("!")
	}

	// test release
	timeout := time.After(2 * time.Second)
	chResult := make(chan bool)
	go func() {
		_ = tr.IterMut()
		chResult <- true
	}()

	select {
	case <-chResult:
		fmt.Println("iterMut finish")
	case <-timeout:
		fmt.Println("iterMut timeout")
		panic("!")
	}
}
