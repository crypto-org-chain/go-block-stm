package block_stm

import "errors"

type Executor struct {
	i         int
	scheduler *Scheduler
	vm        *VM
	mvMemory  *MVMemory
}

func NewExecutor(
	i int,
	scheduler *Scheduler,
	vm *VM,
	mvMemory *MVMemory,
) *Executor {
	return &Executor{
		i:         i,
		scheduler: scheduler,
		vm:        vm,
		mvMemory:  mvMemory,
	}
}

func (e *Executor) Run() {
	var kind TaskKind
	version := InvalidTxnVersion
	for !e.scheduler.Done() {
		if !version.Valid() {
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
		return InvalidTxnVersion, 0
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
	return e.scheduler.FinishValidation(version.Index, aborted)
}
