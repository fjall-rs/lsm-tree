# Unsafe usage

...

## Run fuzz testing

```bash
cd fuzz/data_block
mkdir in
cat /dev/random | head -n 100 > in/input
cargo afl build && cargo afl fuzz -i in -o out target/debug/data_block

cd fuzz/index_block
mkdir in
cat /dev/random | head -n 100 > in/input
cargo afl build && cargo afl fuzz -i in -o out target/debug/index_block

cd fuzz/table_read
mkdir in
cat /dev/random | head -n 100 > in/input
cargo afl build && cargo afl fuzz -i in -o out target/debug/table_read

cd fuzz/compare_prefixed_slice
mkdir in
cat /dev/random | head -n 100 > in/input
cargo afl build && cargo afl fuzz -i in -o out target/debug/compare_prefixed_slice

cd fuzz/prefix_filter
mkdir in
cat /dev/random | head -n 100 > in/input
cargo afl build && cargo afl fuzz -i in -o out target/debug/prefix_filter
```

## Run mutation testing

```bash
cargo-mutants mutants
```
