import json
import matplotlib.pyplot as plt
from collections import defaultdict
from pathlib import Path
from palettable.tableau import BlueRed_6

colors = BlueRed_6.mpl_colors

# Path to the JSONL file
jsonl_path = Path('data.jsonl')

# Data structure: {(impl, unsafe): [(fpr, ns), ...]}
data = defaultdict(list)

# Read the JSONL file
for line in jsonl_path.read_text().splitlines():
    obj = json.loads(line)
    key = (obj['impl'], obj['unsafe'])
    data[key].append((obj['fpr'], obj['ns']))

plt.rcParams.update({
   'axes.labelsize': 8,
   'font.size': 8,
   'legend.fontsize': 10,
   'xtick.labelsize': 10,
   'ytick.labelsize': 10,
   'text.usetex': False,
   'figure.figsize': [4.5, 4.5]
})

# Plotting
plt.figure(figsize=(6, 4))

i = 0

for (impl, unsafe), values in data.items():
    # Sort by FPR for consistent line plots
    values.sort()
    fprs = [fpr for fpr, ns in values]
    ns_vals = [ns for fpr, ns in values]
    safe_label = "unsafe" if unsafe else "safe"
    label = f"{impl}, {safe_label}"
    stroke = "-." if unsafe else "-"
    marker = "v" if impl == "blocked" else "o"
    plt.plot(fprs, ns_vals, marker=marker, label=label, color=colors[i], linestyle=stroke)
    i += 1

plt.xscale("log")
plt.ylim(bottom=0)
plt.xlabel("False positive rate")
plt.ylabel("Latency [ns]")
# plt.title("Read Performance vs False Positive Rate")
plt.legend(loc='upper center', fancybox=True, bbox_to_anchor=(0.5, 1.15), shadow=True, ncol=2)
plt.grid(color="0.9", linestyle='--', linewidth=1)
plt.tight_layout()
# plt.show()
plt.savefig("bloom_speed.svg")

