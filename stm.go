package block_stm

import (
	"context"
	"errors"
	"sync"

	storetypes "cosmossdk.io/store/types"
)

func ExecuteBlock(
	ctx context.Context,
	blockSize int,
	stores []storetypes.StoreKey,
	storage MultiStore,
	executors int,
	executeFn ExecuteFn,
) error {
	// Create a new scheduler
	scheduler := NewScheduler(blockSize)
	mvMemory := NewMVMemory(blockSize, stores)

	var wg sync.WaitGroup
	wg.Add(executors)
	for i := 0; i < executors; i++ {
		e := NewExecutor(ctx, blockSize, stores, scheduler, storage, executeFn, mvMemory, i)
		go func() {
			defer wg.Done()
			e.Run()
		}()
	}
	wg.Wait()

	if !scheduler.Done() {
		if ctx.Err() != nil {
			// cancelled
			return ctx.Err()
		}

		return errors.New("scheduler did not complete")
	}

	// Write the snapshot into the storage
	mvMemory.WriteSnapshot(storage)
	return nil
}
