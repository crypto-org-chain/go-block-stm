package block_stm

import (
	"sync"
)

func ExecuteBlock(stores []string, storage MultiStore, blk []Tx, executors int) error {
	// Create a new scheduler
	blockSize := len(blk)
	scheduler := NewScheduler(blockSize)
	mv := NewMVMemory(blockSize, stores)
	vm := NewVM(stores, storage, mv, scheduler, blk)

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

	// Write the snapshot into the storage
	mv.WriteSnapshot(storage)
	return nil
}
