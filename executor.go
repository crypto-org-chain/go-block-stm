package block_stm

type Executor struct {
	scheduler *Scheduler
}

func NewExecutor(scheduler *Scheduler) *Executor {
	return &Executor{
		scheduler: scheduler,
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
	// TODO
	return TxnVersion{-1, 0}, TaskKindValidation
}

func (e *Executor) NeedsReexecution(version TxnVersion) (TxnVersion, TaskKind) {
	// TODO
	return TxnVersion{-1, 0}, TaskKindValidation
}
