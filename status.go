package block_stm

import "sync"

type Status uint

const (
	StatusReadyToExecute Status = iota
	StatusExecuting
	StatusSuspended
	StatusExecuted
	StatusAborting
)

type StatusEntry struct {
	mutex sync.Mutex

	incarnation Incarnation
	status      Status
}

func (s *StatusEntry) Get() (Status, Incarnation) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.status, s.incarnation
}

func (s *StatusEntry) SetExecuting() (Incarnation, bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.status == StatusReadyToExecute {
		s.status = StatusExecuting
		return s.incarnation, true
	}
	return 0, false
}

// SetAborting is called by Scheduler.AddDependency
func (s *StatusEntry) SetAborting() bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.status == StatusExecuted {
		// dependency resolved
		return false
	}
	// previous status must be EXECUTING
	s.status = StatusAborting
	return true
}

func (s *StatusEntry) SetExecuted() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// status must have been EXECUTING, called by Scheduler.FinishExecution
	s.status = StatusExecuted
}

func (s *StatusEntry) TryValidationAbort(incarnation Incarnation) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.incarnation == incarnation && s.status == StatusExecuted {
		s.status = StatusAborting
		return true
	}
	return false
}
