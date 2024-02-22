package block_stm

type Executor struct {
	i         int
	blockSize int
	stores    []string
	scheduler *Scheduler
	storage   MultiStore
	executeFn ExecuteFn
	mvMemory  *MVMemory
}

func NewExecutor(
	i int,
	blockSize int,
	stores []string,
	scheduler *Scheduler,
	storage MultiStore,
	executeFn ExecuteFn,
	mvMemory *MVMemory,
) *Executor {
	return &Executor{
		i:         i,
		blockSize: blockSize,
		stores:    stores,
		scheduler: scheduler,
		storage:   storage,
		executeFn: executeFn,
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
	if e.scheduler.TryNotify(version.Index) {
		// resumed a suspended transaction
		return InvalidTxnVersion, 0
	}
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
