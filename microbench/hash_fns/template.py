import matplotlib.pyplot as plt
import json
from palettable.tableau import BlueRed_6
from pathlib import Path

colors = BlueRed_6.mpl_colors

data = Path('data.jsonl').read_text()

# Parse the data
data_list = [json.loads(line) for line in data.strip().split('\n')]

# Calculate throughput (hashes per second)
for entry in data_list:
    # Convert ns to seconds and calculate throughput (1 hash per measurement)
    time_in_seconds = entry["ns"] / 1e9
    entry["throughput"] = 1 / time_in_seconds  # 1 hash / time in seconds

# Group data by hash type
grouped_data = {}
for entry in data_list:
    hash_type = entry["hash"]
    if hash_type not in grouped_data:
        grouped_data[hash_type] = {"byte_len": [], "throughput": []}
    grouped_data[hash_type]["byte_len"].append(entry["byte_len"])
    grouped_data[hash_type]["throughput"].append(entry["throughput"])

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

i = 0
markers = ["*", "o", "d", ".", "v", "^"]

for hash_type, values in grouped_data.items():
    plt.plot(values["byte_len"], values["throughput"], marker=markers[i],
             linestyle='-', label=hash_type, color=colors[i])
    i += 1

plt.xlabel("Input length [bytes]")
plt.ylabel("Throughput [op/s]")

plt.xscale('log')
plt.yscale('log')

plt.legend(loc='upper center', fancybox=True, bbox_to_anchor=(0.5, 1.25), shadow=True, ncol=3)
plt.grid(color="0.9", linestyle='--', linewidth=1)
plt.tight_layout()

# Save the plot to a file
plt.savefig("hash_fns.svg")
