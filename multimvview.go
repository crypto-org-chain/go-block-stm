package block_stm

type MultiMVMemoryView struct {
	stores []string
	views  map[string]*MVMemoryView
}

var _ MultiStore = (*MultiMVMemoryView)(nil)

func NewMultiMVMemoryView(
	stores []string, storage MultiStore,
	mvMemory *MVMemory, schedule *Scheduler,
	txn TxnIndex,
) *MultiMVMemoryView {
	views := make(map[string]*MVMemoryView, len(stores))
	for i, name := range stores {
		views[name] = NewMVMemoryView(i, storage.GetKVStore(name), mvMemory, schedule, txn)
	}
	return &MultiMVMemoryView{
		stores: stores,
		views:  views,
	}
}

func (mv *MultiMVMemoryView) GetKVStore(name string) KVStore {
	return mv.views[name]
}

func (s *MultiMVMemoryView) Result() (MultiReadSet, MultiWriteSet) {
	rs := make(MultiReadSet, len(s.views))
	ws := make(MultiWriteSet, len(s.views))
	for i, name := range s.stores {
		view := s.views[name]
		rs[i] = view.readSet
		ws[i] = view.writeSet
	}
	return rs, ws
}
