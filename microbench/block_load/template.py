import matplotlib.pyplot as plt
import json
from palettable.tableau import PurpleGray_6
from pathlib import Path

colors = PurpleGray_6.mpl_colors

data = Path('data.jsonl').read_text()

# Parse the data
data_list = [json.loads(line) for line in data.strip().split('\n')]

# Separate data based on the 'unsafe' field
safe_data = [item for item in data_list if not item["unsafe"]]
unsafe_data = [item for item in data_list if item["unsafe"]]

# Extract x and y values for each category
safe_block_sizes = [item["block_size"] for item in safe_data]
safe_latencies = [item["rps_ns"] for item in safe_data]

unsafe_block_sizes = [item["block_size"] for item in unsafe_data]
unsafe_latencies = [item["rps_ns"] for item in unsafe_data]

plt.rcParams.update({
   'axes.labelsize': 8,
   'font.size': 8,
   'legend.fontsize': 10,
   'xtick.labelsize': 10,
   'ytick.labelsize': 10,
   'text.usetex': False,
   'figure.figsize': [4.5, 4.5]
})

# Create the plot
plt.figure(figsize=(6, 4))

# Plot the data for 'unsafe' = False
plt.plot(
    safe_block_sizes,
    safe_latencies,
    marker="o",
    linestyle="-",
    label="safe",
    color=colors[0],
)

# Plot the data for 'unsafe' = True
plt.plot(
    unsafe_block_sizes,
    unsafe_latencies,
    marker="s",
    linestyle="--",
    label="unsafe",
    color=colors[1],
)

# Add labels and title
plt.xscale("log")
plt.yscale("log")
# plt.ylim(bottom=0)
plt.xlabel("Block size [bytes]")
plt.ylabel("Read latency [ns/op]")
plt.legend(loc='upper center', fancybox=True, bbox_to_anchor=(0.5, 1.05), shadow=True, ncol=2)
plt.grid(color="0.9", linestyle='--', linewidth=1)
plt.tight_layout()

# Show the plot
plt.savefig("block_load.svg")
