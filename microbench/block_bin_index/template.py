import json
from pathlib import Path
import matplotlib.pyplot as plt
from palettable.tableau import PurpleGray_6

colors = PurpleGray_6.mpl_colors

# Path to your data file
data_path = Path("data.jsonl")

# Read and parse the data
safe_data = []
unsafe_data = []

with data_path.open() as f:
    for line in f:
        if len(line) == 0:
            continue

        entry = json.loads(line)

        if entry.get("item_count") != 1000:
            continue

        if entry.get("unsafe") == False:
            safe_data.append(entry)
        else:
            unsafe_data.append(entry)

# Sort by restart_interval to ensure smooth lines
safe_data.sort(key=lambda x: x["restart_interval"])
unsafe_data.sort(key=lambda x: x["restart_interval"])

# Extract data for plotting
restart_interval_safe = [d["restart_interval"] for d in safe_data]
rps_ns_safe = [d["rps_ns"] for d in safe_data]
block_size = [d["block_size"] for d in safe_data]

restart_interval_unsafe = [d["restart_interval"] for d in unsafe_data]
rps_ns_unsafe = [d["rps_ns"] for d in unsafe_data]

# Create figure and first Y-axis
fig, ax1 = plt.subplots(figsize=(6, 4))

# Plot rps_ns (left Y-axis)
ax1.plot(restart_interval_safe, rps_ns_safe, label='Read latency (safe)', marker='o', color = colors[0])
ax1.plot(restart_interval_unsafe, rps_ns_unsafe, label='Read latency (unsafe)', marker='x', color = colors[1])
ax1.set_xlabel('Restart interval')
ax1.set_ylabel('Point read latency [ns]')
ax1.tick_params(axis='y')

# Create second Y-axis for block size
ax2 = ax1.twinx()
ax2.plot(restart_interval_safe, block_size, label='Block size', linestyle='--', marker='d', color = colors[2])
ax2.set_ylabel('Block size [bytes]')
ax2.tick_params(axis='y')

# Combine legends from both axes
lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper center', fancybox=True, bbox_to_anchor=(0.5, 1.25), shadow=True, ncol=2)

# Grid and title
ax1.grid(color="0.9", linestyle='--', linewidth=1)
# plt.title('Safe vs Unsafe: rps_ns and Block Size vs Hash Ratio')
plt.tight_layout()

plt.savefig("block_binary_index.svg")
