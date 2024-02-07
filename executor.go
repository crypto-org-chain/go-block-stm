package block_stm

import "errors"

type Executor struct {
	scheduler *Scheduler
	vm        *VM
	mvMemory  *MVMemory
}

func NewExecutor(block_size int, vm *VM) *Executor {
	return &Executor{
		scheduler: NewScheduler(block_size),
		vm:        vm,
		mvMemory:  NewMVMemory(block_size),
	}
}

func (e *Executor) Run() {
	var kind TaskKind
	version := TxnVersion{-1, 0}
	for !e.scheduler.Done() {
		if !version.IsValid() {
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
	result, err := e.vm.Execute(version.Index)
	var readErr ErrReadError
	if errors.As(err, &readErr) {
		if !e.scheduler.AddDependency(version.Index, readErr.BlockingTxn) {
			// dependency resolved in the meantime, re-execute
			return e.TryExecute(version)
		}
		return TxnVersion{-1, 0}, 0
	}

	wroteNewLocation := e.mvMemory.Record(version, result.ReadSet, result.WriteSet)
	return e.scheduler.FinishExecution(version, wroteNewLocation)
}

func (e *Executor) NeedsReexecution(version TxnVersion) (TxnVersion, TaskKind) {
	valid := e.mvMemory.ValidateReadSet(version.Index)
	aborted := !valid && e.scheduler.TryValidationAbort(version)
	if aborted {
		e.mvMemory.ConvertWritesToEstimates(version.Index)
	}
	return e.scheduler.FinishValidation(version.Index, !aborted)
}
