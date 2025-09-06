rm -f data.jsonl
cargo run -r --features use_unsafe | save data.jsonl --append
cargo run -r --no-default-features | save data.jsonl --append
python3 template.py
