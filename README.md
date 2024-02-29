`go-block-stm` implements the [block-stm algorithm](https://arxiv.org/abs/2203.06871), it follows the paper pseudocode pretty closely.

Main derivations from the paper are:

### Optimisation

We applied the optimization described in section 4 of the paper:

```
Block-STM calls add_dependency from the VM itself, and can thus re-read and continue execution when false is returned.
```

When the VM execution reads an `ESTIMATE` mark, it'll hang on a `CondVar`, so it can resume execution after the dependency is resolved,
much more efficient than abortion and rerun.

### Support Deletion, Iteration, and MultiStore

These features are necessary for integration with cosmos-sdk.

The multi-version data structure is implemented with nested btree for easier iteration support, 
the `WriteSet` is also implemented with a btree, and it takes advantage of ordered property to optimize some logic.

The internal data structures are also adapted with multiple stores in mind.
