package block_stm

import (
	"context"

	storetypes "cosmossdk.io/store/types"
)

type Executor struct {
	ctx       context.Context
	blockSize int
	stores    []storetypes.StoreKey
	scheduler *Scheduler
	storage   MultiStore
	executeFn ExecuteFn
	mvMemory  *MVMemory

	i int
}

func NewExecutor(
	ctx context.Context,
	blockSize int,
	stores []storetypes.StoreKey,
	scheduler *Scheduler,
	storage MultiStore,
	executeFn ExecuteFn,
	mvMemory *MVMemory,
	i int,
) *Executor {
	return &Executor{
		ctx:       ctx,
		blockSize: blockSize,
		stores:    stores,
		scheduler: scheduler,
		storage:   storage,
		executeFn: executeFn,
		mvMemory:  mvMemory,
		i:         i,
	}
}

// Invariant `num_active_tasks`:
//   - `NextTask` increases it if returns a valid task.
//   - `TryExecute` and `NeedsReexecution` don't change it if it returns a new valid task to run,
//     otherwise it decreases it.
func (e *Executor) Run() {
	var kind TaskKind
	version := InvalidTxnVersion
	for !e.scheduler.Done() {
		if !version.Valid() {
			// check for cancellation
			select {
			case <-e.ctx.Done():
				return
			default:
			}

			version, kind = e.scheduler.NextTask()
			continue
		}

		switch kind {
		case TaskKindExecution:
			version, kind = e.TryExecute(version)
		case TaskKindValidation:
			version, kind = e.NeedsReexecution(version)
		}
	}
}

func (e *Executor) TryExecute(version TxnVersion) (TxnVersion, TaskKind) {
	e.scheduler.executedTxns.Add(1)
	readSet, writeSet := e.execute(version.Index)
	wroteNewLocation := e.mvMemory.Record(version, readSet, writeSet)
	return e.scheduler.FinishExecution(version, wroteNewLocation)
}

func (e *Executor) NeedsReexecution(version TxnVersion) (TxnVersion, TaskKind) {
	e.scheduler.validatedTxns.Add(1)
	valid := e.mvMemory.ValidateReadSet(version.Index)
	aborted := !valid && e.scheduler.TryValidationAbort(version)
	if aborted {
		e.mvMemory.ConvertWritesToEstimates(version.Index)
	}
	return e.scheduler.FinishValidation(version.Index, aborted)
}

func (e *Executor) execute(txn TxnIndex) (MultiReadSet, MultiWriteSet) {
	view := NewMultiMVMemoryView(e.stores, e.storage, e.mvMemory, e.scheduler, txn)
	e.executeFn(txn, view)
	return view.Result()
}
