rm -f data.jsonl
cargo run -r | save data.jsonl --append
python3 template.py
