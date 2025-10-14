#!/bin/bash

cargo afl build -r
cat /dev/random | head -n 1024 > in1/in1
cat /dev/random | head -n 1024 > in2/in2
cat /dev/random | head -n 1024 > in3/in3
cat /dev/random | head -n 1024 > in4/in4

# Set session name
SESSION_NAME="my_session"

# Start a new tmux session in detached mode
tmux new-session -d -s $SESSION_NAME -c "w"

# Split the first window vertically
tmux split-window -h -p 25 -t $SESSION_NAME -c $1

# Focus on the left pane and start helix
tmux select-pane -t 1
# tmux send-keys "cargo afl fuzz -i in1 -o out1 target/release/data_block" C-m

# Switch focus to the right pane
tmux select-pane -t 2
# tmux send-keys "cargo afl fuzz -i in2 -o out2 target/release/data_block" C-m

# Create a new window for RSB
# tmux new-window -t $SESSION_NAME -n "2" -c "/devssd/code/rust/rust-storage-bench"

# Attach to the tmux session
tmux attach -t $SESSION_NAME
