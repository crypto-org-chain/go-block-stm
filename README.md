`go-block-stm` implements the [block-stm algorithm](https://arxiv.org/abs/2203.06871), it follows the paper pseudocode pretty closely.

### Optimisation

We applied the optimization described in section 4 of the paper:

```
Block-STM calls add_dependency from the VM itself, and can thus re-read and continue execution when false is returned.
```

This reduces transaction re-runs significantly.

### Deletion Support

We support deletion API to transaction.

### MultiStore Support

To integrate with cosmos-sdk, the data structures is adapted with multiple stores in mind.

### Iteration Support

Iteration is another necessary feature for cosmos-sdk integration.
