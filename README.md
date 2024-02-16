`go-block-stm` implements the [block-stm algorithm](), it follows the paper pseudocode pretty closely.

### Deletion

- Final storage don't support `nil` as value.
- Overlay storage re-use the `nil` state as deletion.
