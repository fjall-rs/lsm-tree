# Unsafe usage

...

## Run fuzz testing

```bash
cargo +nightly fuzz run data_block -- -max_len=8000000
cargo +nightly fuzz run index_block -- -max_len=8000000
cargo +nightly fuzz run partition_point -- -max_len=1000000
```
