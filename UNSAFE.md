# Unsafe usage

Currently, the project itself only uses one **1** unsafe block (ignoring dependencies which are tested themselves separately):

- https://github.com/fjall-rs/lsm-tree/blob/2d8686e873369bd9c4ff2b562ed988c1cea38331/src/binary_search.rs#L23-L25

## Run fuzz testing

```bash
cargo +nightly fuzz run data_block -- -max_len=8000000
cargo +nightly fuzz run partition_point -- -max_len=1000000
```
