package block_stm

import "sync"

type Status uint

const (
	StatusReadyToExecute Status = iota
	StatusExecuting
	StatusExecuted
	StatusAborting
)

type StatusEntry struct {
	mutex sync.Mutex

	incarnation Incarnation
	status      Status
}

func (s *StatusEntry) IsExecuted() (ok bool, incarnation Incarnation) {
	s.mutex.Lock()
	if s.status == StatusExecuted {
		ok = true
		incarnation = s.incarnation
	}
	s.mutex.Unlock()
	return
}

func (s *StatusEntry) TrySetExecuting() (Incarnation, bool) {
	s.mutex.Lock()

	if s.status == StatusReadyToExecute {
		s.status = StatusExecuting
		incarnation := s.incarnation
		s.mutex.Unlock()
		return incarnation, true
	}
	s.mutex.Unlock()
	return 0, false
}

func (s *StatusEntry) SetStatus(status Status) {
	s.mutex.Lock()
	s.status = status
	s.mutex.Unlock()
}

func (s *StatusEntry) TryValidationAbort(incarnation Incarnation) bool {
	s.mutex.Lock()

	if s.incarnation == incarnation && s.status == StatusExecuted {
		s.status = StatusAborting
		s.mutex.Unlock()
		return true
	}
	s.mutex.Unlock()
	return false
}

func (s *StatusEntry) SetReadyStatus() {
	s.mutex.Lock()
	s.incarnation++
	// status must be ABORTING
	s.status = StatusReadyToExecute
	s.mutex.Unlock()
}
