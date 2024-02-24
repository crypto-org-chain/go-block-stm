package block_stm

import (
	"sync"

	storetypes "cosmossdk.io/store/types"
)

func ExecuteBlock(blockSize int, stores []storetypes.StoreKey, storage MultiStore, executors int, executeFn ExecuteFn) {
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
