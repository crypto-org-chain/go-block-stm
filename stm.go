package block_stm

import (
	"sync"
)

func ExecuteBlock(storage KVStore, blk []Tx, executors int) error {
	// Create a new scheduler
	blockSize := len(blk)
	scheduler := NewScheduler(blockSize)
	mv := NewMVMemory(blockSize)
	vm := NewVM(storage, mv, scheduler, blk)

	wg := sync.WaitGroup{}
	wg.Add(executors)
	for i := 0; i < executors; i++ {
		i := i
		go func() {
			defer wg.Done()
			NewExecutor(i, scheduler, vm, mv).Run()
		}()
	}
	wg.Wait()

	// fmt.Println("stats", scheduler.Stats())

	// Write the snapshot into the storage
	for _, pair := range mv.Snapshot() {
		storage.Set(pair.Key, pair.Value)
	}
	return nil
}
