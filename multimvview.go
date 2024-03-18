package block_stm

import storetypes "cosmossdk.io/store/types"

type MultiMVMemoryView struct {
	stores    map[storetypes.StoreKey]int
	views     []*MVMemoryView
	txn       TxnIndex
	storage   MultiStore
	mvMemory  *MVMemory
	scheduler *Scheduler
}

var _ MultiStore = (*MultiMVMemoryView)(nil)

func NewMultiMVMemoryView(
	stores map[storetypes.StoreKey]int,
	storage MultiStore,
	mvMemory *MVMemory,
	scheduler *Scheduler,
	txn TxnIndex,
) *MultiMVMemoryView {
	return &MultiMVMemoryView{
		stores:    stores,
		views:     make([]*MVMemoryView, len(stores)),
		txn:       txn,
		storage:   storage,
		mvMemory:  mvMemory,
		scheduler: scheduler,
	}
}

func (mv *MultiMVMemoryView) GetKVStore(name storetypes.StoreKey) storetypes.KVStore {
	i, ok := mv.stores[name]
	if !ok {
		return nil
	}
	if mv.views[i] == nil {
		mv.views[i] = NewMVMemoryView(i, mv.storage.GetKVStore(name), mv.mvMemory, mv.scheduler, mv.txn)
	}
	return mv.views[i]
}

func (s *MultiMVMemoryView) Result() (MultiReadSet, MultiWriteSet) {
	resReads := make(MultiReadSet, len(s.views))
	resWrites := make(MultiWriteSet, len(s.views))
	for i, view := range s.views {
		if view != nil {
			resReads[i] = view.readSet
			resWrites[i] = view.writeSet
		}
	}
	return resReads, resWrites
}
