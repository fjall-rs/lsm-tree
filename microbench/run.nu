let benchmarks = [
  "block_bin_index",
  "block_hash_index",
  "block_load",
  "bloom_fpr",
  "bloom_speed",
  "fractional_cascading",
  "hash_fns",
]

print "===== Running all benchmarks, this will take a while ====="

for bench in $benchmarks {
  print $"=== Running ($bench) function benchmark ==="
  cd $bench
  nu run.nu
  cd ..
}
