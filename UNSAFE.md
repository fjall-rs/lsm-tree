# Unsafe usage

...

## Run fuzz testing

```bash
cd fuzz/data_block
mkdir in
cat /dev/random | head -n 100 > in/input
cargo afl build && cargo afl fuzz -i in -o out target/debug/data_block
```

## Run mutation testing

```bash
cargo-mutants mutants --test-tool=nextest
```
