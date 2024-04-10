`go-block-stm` implements the [block-stm algorithm](https://arxiv.org/abs/2203.06871), it follows the paper pseudocode pretty closely.

The main API is a simple function call [ExecuteBlock](https://github.com/yihuang/go-block-stm/blob/main/stm.go#L10):

```golang
type ExecuteFn func(TxnIndex, MultiStore)
func ExecuteBlock(
	ctx context.Context,           // context for cancellation
	blockSize int,                 // the number of the transactions to be executed
	stores []storetypes.StoreKey,  // the list of store keys to support
	storage MultiStore,            // the parent storage, after all transactions are executed, the whole change sets are written into parent storage at once
	executors int,                 // how many concurrent executors to spawn
	executeFn ExecuteFn,           // callback function to actually execute a transaction with a wrapped `MultiStore`.
) error
```

The main deviations from the paper are:

### Suspend On Estimate Mark

We applied the optimization described in section 4 of the paper:

```
Block-STM calls add_dependency from the VM itself, and can thus re-read and continue execution when false is returned.
```

When the VM execution reads an `ESTIMATE` mark, it'll hang on a `CondVar`, so it can resume execution after the dependency is resolved,
much more efficient than abortion and rerun.

### Support Deletion

cosmos-sdk don't allow setting a `nil` value, so we reuse the `nil` for tombstone value, so `Delete` can be implemented as a special case of `Set` with `nil` value.

### Support Iteration, and MultiStore

These features are necessary for integration with cosmos-sdk.

The multi-version data structure is implemented with nested btree for easier iteration support, 
the `WriteSet` is also implemented with a btree, and it takes advantage of ordered property to optimize some logic.

The internal data structures are also adapted with multiple stores in mind.

### Concurrency Friendly `Has` Operation

The `Has(key)` operation is usually implemeneted as `Get(key) != nil` naively, but it can be implemented more friendly
to concurrency than `Get` operation, because it only observe the existence status of the key, not the content of the value ifself, so we treat it differently, `Get` operation is validated by checking the version, but `Has` operation will validates the value existence itself. So for example, if a key is updated by another transaction, it won't abort the transaction that only observed the key with `Has` operation, because the existence status is not changed.
