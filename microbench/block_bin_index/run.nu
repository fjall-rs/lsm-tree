rm -f data.jsonl
cargo run -r | save --append data.jsonl
cargo run -r --features use_unsafe | save --append data.jsonl
python3 template3d_speed.py
python3 template3d_space.py
python3 template.py
