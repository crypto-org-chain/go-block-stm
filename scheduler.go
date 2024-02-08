package block_stm

import (
	"sync"
	"sync/atomic"
)

type TaskKind int

const (
	TaskKindExecution TaskKind = iota
	TaskKindValidation
)

type TxDependency struct {
	mutex      sync.Mutex
	dependents []TxnIndex
}

func (t *TxDependency) Swap(new []TxnIndex) []TxnIndex {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	old := t.dependents
	t.dependents = new
	return old
}

// Scheduler implements the scheduler for the block-stm
// ref: `Algorithm 4 The Scheduler module, variables, utility APIs and next task logic`
type Scheduler struct {
	block_size int

	// An index that tracks the next transaction to try and execute.
	execution_idx atomic.Uint64
	// A similar index for tracking validation.
	validation_idx atomic.Uint64
	// Number of times validation_idx or execution_idx was decreased
	decrease_cnt atomic.Uint64
	// Number of ongoing validation and execution tasks
	num_active_tasks atomic.Uint64
	// Marker for completion
	done_marker atomic.Bool

	// txn_idx to a mutex-protected set of dependent transaction indices
	txn_dependency []TxDependency
	// txn_idx to a mutex-protected pair (incarnation_number, status), where status ∈ {READY_TO_EXECUTE, EXECUTING, EXECUTED, ABORTING}.
	txn_status []StatusEntry
}

func NewScheduler(block_size int) *Scheduler {
	return &Scheduler{
		block_size:     block_size,
		txn_dependency: make([]TxDependency, block_size),
		txn_status:     make([]StatusEntry, block_size),
	}
}

func (s *Scheduler) Done() bool {
	return s.done_marker.Load()
}

func (s *Scheduler) DecreaseExecutionIdx(target TxnIndex) {
	StoreMin(&s.execution_idx, uint64(target))
	s.decrease_cnt.Add(1)
}

func (s *Scheduler) DecreaseValidationIdx(target TxnIndex) {
	StoreMin(&s.validation_idx, uint64(target))
	s.decrease_cnt.Add(1)
}

func (s *Scheduler) CheckDone() {
	observed_cnt := s.decrease_cnt.Load()
	if s.execution_idx.Load() >= uint64(s.block_size) &&
		s.validation_idx.Load() >= uint64(s.block_size) &&
		s.num_active_tasks.Load() == 0 {
		if observed_cnt == s.decrease_cnt.Load() {
			s.done_marker.Store(true)
		}
	}
}

func (s *Scheduler) TryIncarnate(idx TxnIndex) (Incarnation, bool) {
	if int(idx) < s.block_size {
		incarnation, ok := s.txn_status[idx].SetExecuting()
		if ok {
			return incarnation, true
		}
	}
	DecreaseAtomic(&s.num_active_tasks)
	return 0, false
}

// NextVersionToExecute get the next transaction index to execute,
// returns invalid version if no task is available
func (s *Scheduler) NextVersionToExecute() TxnVersion {
	execution_idx := s.execution_idx.Load()
	if execution_idx >= uint64(s.block_size) {
		s.CheckDone()
		return InvalidTxnVersion
	}
	IncreaseAtomic(&s.num_active_tasks)
	incarnation, ok := s.TryIncarnate(TxnIndex(s.execution_idx.Add(1)))
	if !ok {
		return InvalidTxnVersion
	}
	return TxnVersion{TxnIndex(execution_idx), incarnation}
}

// NextVersionToValidate get the next transaction index to validate,
// returns invalid version if no task is available
func (s *Scheduler) NextVersionToValidate() TxnVersion {
	if s.validation_idx.Load() >= uint64(s.block_size) {
		s.CheckDone()
		return InvalidTxnVersion
	}
	IncreaseAtomic(&s.num_active_tasks)
	idx_to_validate := s.validation_idx.Add(1)
	if idx_to_validate < uint64(s.block_size) {
		status, incarnation := s.txn_status[idx_to_validate].Get()
		if status == StatusExecuted {
			return TxnVersion{TxnIndex(idx_to_validate), incarnation}
		}
	}

	DecreaseAtomic(&s.num_active_tasks)
	return InvalidTxnVersion
}

// NextTask returns the transaction index and task kind for the next task to execute or validate,
// returns invalid version if no task is available.
func (s *Scheduler) NextTask() (TxnVersion, TaskKind) {
	validation_idx := s.validation_idx.Load()
	execution_idx := s.execution_idx.Load()
	if validation_idx < execution_idx {
		return s.NextVersionToValidate(), TaskKindValidation
	} else {
		return s.NextVersionToExecute(), TaskKindExecution
	}
}

// AddDependency adds a dependency between two transactions, returns false if tx is already executed
func (s *Scheduler) AddDependency(txn TxnIndex, blocking_txn TxnIndex) bool {
	entry := &s.txn_dependency[blocking_txn]
	entry.mutex.Lock()
	defer entry.mutex.Unlock()

	if !s.txn_status[txn].SetAborting() {
		return false
	}

	entry.dependents = append(entry.dependents, txn)
	DecreaseAtomic(&s.num_active_tasks)
	return true
}

func (s *Scheduler) SetReadyStatus(txn TxnIndex) {
	entry := &s.txn_status[txn]

	entry.mutex.Lock()
	defer entry.mutex.Unlock()

	entry.incarnation++
	// status must be ABORTING
	entry.status = StatusReadyToExecute
}

func (s *Scheduler) ResumeDependencies(txns []TxnIndex) {
	var minIdx TxnIndex = -1
	for _, txn := range txns {
		s.SetReadyStatus(txn)
		if txn < minIdx {
			minIdx = txn
		}
	}

	if minIdx >= 0 {
		s.DecreaseExecutionIdx(minIdx)
	}
}

func (s *Scheduler) FinishExecution(version TxnVersion, wroteNewPath bool) (TxnVersion, TaskKind) {
	s.txn_status[version.Index].SetExecuted()
	deps := s.txn_dependency[version.Index].Swap(nil)
	s.ResumeDependencies(deps)
	if s.validation_idx.Load() > uint64(version.Index) {
		if !wroteNewPath {
			return version, TaskKindValidation
		}
		s.DecreaseValidationIdx(version.Index)
	}
	DecreaseAtomic(&s.num_active_tasks)
	return InvalidTxnVersion, 0
}

func (s *Scheduler) TryValidationAbort(version TxnVersion) bool {
	return s.txn_status[version.Index].TryValidationAbort(version.Incarnation)
}

func (s *Scheduler) FinishValidation(txn TxnIndex, aborted bool) (TxnVersion, TaskKind) {
	if aborted {
		s.SetReadyStatus(txn)
		s.DecreaseValidationIdx(txn + 1)
		if s.execution_idx.Load() > uint64(txn) {
			incarnation, ok := s.TryIncarnate(txn)
			if ok {
				return TxnVersion{txn, incarnation}, TaskKindExecution
			}
			// TryIncarnate already decresed num_active_tasks, so no need to decrease it again
			return InvalidTxnVersion, 0
		}
	}

	DecreaseAtomic(&s.num_active_tasks)
	return InvalidTxnVersion, 0
}
