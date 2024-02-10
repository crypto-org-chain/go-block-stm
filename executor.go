package block_stm

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
	e.scheduler.executedTxns.Add(1)
	result, err := e.vm.Execute(version.Index)
	if readErr, ok := err.(ErrReadError); ok { // TODO efficient read error handling
		e.scheduler.readErrTxns.Add(1)
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
	e.scheduler.validatedTxns.Add(1)
	valid := e.mvMemory.ValidateReadSet(version.Index)
	aborted := !valid && e.scheduler.TryValidationAbort(version)
	if aborted {
		e.scheduler.abortedTxns.Add(1)
		e.mvMemory.ConvertWritesToEstimates(version.Index)
	}
	return e.scheduler.FinishValidation(version.Index, aborted)
}
