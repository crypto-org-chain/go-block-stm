package block_stm

type MultiMVMemoryView struct {
	storeIndices map[string]int
	views        []MVMemoryView
}

var _ MultiStore = (*MultiMVMemoryView)(nil)

func NewMultiMVMemoryView(stores []string, storage MultiStore, mvMemory *MVMemory, schedule *Scheduler, txn TxnIndex) *MultiMVMemoryView {
	views := make([]MVMemoryView, len(stores))
	storeIndices := make(map[string]int, len(stores))
	for i, name := range stores {
		storeIndices[name] = i
		views[i] = *NewMVMemoryView(i, storage.GetKVStore(name), mvMemory, schedule, txn)
	}
	return &MultiMVMemoryView{
		storeIndices: storeIndices,
		views:        views,
	}
}

func (mv *MultiMVMemoryView) GetKVStore(name string) KVStore {
	return &mv.views[mv.storeIndices[name]]
}

func (s *MultiMVMemoryView) Result() (MultiReadSet, MultiWriteSet) {
	rs := make(MultiReadSet, len(s.views))
	ws := make(MultiWriteSet, len(s.views))
	for i, view := range s.views {
		rs[i] = view.readSet
		ws[i] = view.writeSet
	}
	return rs, ws
}
