package block_stm

import "sync"

type Status uint

const (
	StatusReadyToExecute Status = iota
	StatusExecuting
	StatusExecuted
	StatusAborting
	StatusSuspended
)

type StatusEntry struct {
	sync.Mutex

	incarnation Incarnation
	status      Status

	cond *Condvar
}

func (s *StatusEntry) IsExecuted() (ok bool, incarnation Incarnation) {
	s.Lock()
	if s.status == StatusExecuted {
		ok = true
		incarnation = s.incarnation
	}
	s.Unlock()
	return
}

func (s *StatusEntry) TrySetExecuting() (Incarnation, bool) {
	s.Lock()

	if s.status == StatusReadyToExecute {
		s.status = StatusExecuting
		incarnation := s.incarnation
		s.Unlock()
		return incarnation, true
	}
	s.Unlock()
	return 0, false
}

func (s *StatusEntry) SetStatus(status Status) {
	s.Lock()
	s.status = status
	s.Unlock()
}

func (s *StatusEntry) TryValidationAbort(incarnation Incarnation) bool {
	s.Lock()

	if s.incarnation == incarnation && s.status == StatusExecuted {
		s.status = StatusAborting
		s.Unlock()
		return true
	}
	s.Unlock()
	return false
}

func (s *StatusEntry) SetReadyStatus() {
	s.Lock()
	s.incarnation++
	// status must be ABORTING
	s.status = StatusReadyToExecute
	s.Unlock()
}

func (s *StatusEntry) Suspend(cond *Condvar) {
	s.Lock()
	s.cond = cond
	s.status = StatusSuspended
	s.Unlock()
}

func (s *StatusEntry) TryNotify() bool {
	s.Lock()
	if s.cond == nil {
		s.Unlock()
		return false
	}

	s.cond.Notify()
	s.cond = nil
	s.Unlock()
	return true
}
