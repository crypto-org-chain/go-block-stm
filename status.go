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

func (s *StatusEntry) Get() (status Status, incarnation Incarnation) {
	s.mutex.Lock()
	status, incarnation = s.status, s.incarnation
	s.mutex.Unlock()
	return
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

func (s *StatusEntry) SetStatus(status Status) {
	s.mutex.Lock()
	s.status = status
	s.mutex.Unlock()
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
