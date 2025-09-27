import matplotlib.pyplot as plt
import json
from palettable.tableau import PurpleGray_6
from pathlib import Path

colors = PurpleGray_6.mpl_colors

data = Path("data.jsonl").read_text()

# Parse the data
data_list = [json.loads(line) for line in data.strip().split('\n')]

# Organize data by boolean key
from collections import defaultdict

grouped = defaultdict(list)
for entry in data_list:
    key = (entry["unsafe"], entry["std_partition_point"], entry["cascading"])
    grouped[key].append((entry["lmax_ssts"], entry["ns"]))

# Plot
plt.figure(figsize=(6, 4))

markers = ["*", "o", "d", ".", "v", "^"]
i = 0

for key, values in grouped.items():
    values.sort()
    x = [v[0] for v in values]
    y = [v[1] for v in values]
    label = "Cascading" if key[2] else "No cascading"
    label += " unsafe" if key[0] else ""
    plt.plot(x, y, label=label, color=colors[i], marker=markers[i])
    i += 1

plt.xscale("log")

plt.xlabel("Segments in last level")
plt.ylabel("lookup latency [ns]")

plt.legend(loc='upper center', fancybox=True, bbox_to_anchor=(0.5, 1.10), shadow=True, ncol=2)
plt.grid(color="0.9", linestyle='--', linewidth=1)
plt.tight_layout()

plt.savefig("segment_indexing.svg")

