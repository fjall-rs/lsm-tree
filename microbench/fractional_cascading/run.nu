#/bin/nu

rm -f data.jsonl
cargo run -r | save data.jsonl --append
cargo run -r --features use_unsafe | save data.jsonl --append
cargo run -r --features cascading | save data.jsonl --append
cargo run -r --features cascading,use_unsafe | save data.jsonl --append
python3 template.py
