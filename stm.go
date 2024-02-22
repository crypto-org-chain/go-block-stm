package block_stm

import (
	"sync"
)

func ExecuteBlock(blockSize int, stores []string, storage MultiStore, executeFn ExecuteFn, executors int) {
	// Create a new scheduler
	scheduler := NewScheduler(blockSize)
	mvMemory := NewMVMemory(blockSize, stores)

	wg := sync.WaitGroup{}
	wg.Add(executors)
	for i := 0; i < executors; i++ {
		i := i
		go func() {
			defer wg.Done()
			NewExecutor(i, blockSize, stores, scheduler, storage, executeFn, mvMemory).Run()
		}()
	}
	wg.Wait()

	// Write the snapshot into the storage
	mvMemory.WriteSnapshot(storage)
}
