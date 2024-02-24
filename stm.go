package block_stm

import (
	"context"

	storetypes "cosmossdk.io/store/types"
	"golang.org/x/sync/errgroup"
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

	var eg errgroup.Group
	for i := 0; i < executors; i++ {
		i := i
		eg.Go(func() error {
			return NewExecutor(i, blockSize, stores, scheduler, storage, executeFn, mvMemory).Run(ctx)
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}

	// Write the snapshot into the storage
	mvMemory.WriteSnapshot(storage)
	return nil
}
