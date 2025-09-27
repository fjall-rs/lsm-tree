import json
import matplotlib.pyplot as plt
from collections import defaultdict
from pathlib import Path
from palettable.tableau import PurpleGray_6

colors = PurpleGray_6.mpl_colors

jsonl_path = Path('data.jsonl')

fpr_data = defaultdict(list)
size_data = defaultdict(list)

for line in jsonl_path.read_text().splitlines():
    obj = json.loads(line)
    impl = obj['impl']
    fpr_data[impl].append((obj['target_fpr'], obj['real_fpr']))
    size_data[impl].append((obj['target_fpr'], obj['bytes']))

plt.rcParams.update({
   'axes.labelsize': 8,
   'font.size': 8,
   'legend.fontsize': 10,
   'xtick.labelsize': 10,
   'ytick.labelsize': 10,
   'text.usetex': False,
   'figure.figsize': [4.5, 4.5]
})

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(8, 4))

# --- Plot 1: Real FPR vs Target FPR ---
i = 0

for impl, values in fpr_data.items():
    values.sort()
    x_vals = [x for x, y in values]
    y_vals = [y for x, y in values]
    marker = "v" if impl == "blocked" else "o"
    label = impl
    ax1.plot(x_vals, y_vals, marker=marker, label=label, color=colors[i], linestyle="-")
    i += 1

# --- Plot 2: Filter Size vs Target FPR ---
i = 0
for impl, values in size_data.items():
    values.sort()
    x_vals = [x for x, y in values]
    y_vals = [y / 1_024 / 1_024 for x, y in values]
    marker = "v" if impl == "blocked" else "o"
    ax2.plot(x_vals, y_vals, marker=marker, label=impl, color=colors[i], linestyle="-")
    i += 1

# --- Secondary Y-axis: Size difference ---
ax2b = ax2.twinx()

# Compute difference (impls[1] - impls[0]) assuming same target_fpr
impl1_vals = sorted(size_data["standard"])
impl2_vals = sorted(size_data["blocked"])

# Make sure lengths and x-values match
percent_diff_x = []
percent_diff_y = []
for (x1, y1), (x2, y2) in zip(impl1_vals, impl2_vals):
    percent_diff_x.append(x1)
    percent_diff_y.append(100.0 * (y2 - y1) / y1)

ax2b.plot(percent_diff_x, percent_diff_y, color='#a0a0a0', linestyle='dotted', marker='x', label="Diff")
ax2b.set_ylabel("Size difference [%]")
ax2b.invert_yaxis()
ax2b.set_ylim(top=0, bottom=33)
ax2b.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x)}"))

ax1.set_title("A", loc='left')
ax1.set_xscale("log")
ax1.set_yscale("log")
ax1.set_xlabel("Target false positive rate")
ax1.set_ylabel("Real false positive rate")
ax1.grid(color="0.9", linestyle='--', linewidth=1)
ax1.legend(loc='upper center', fancybox=True, bbox_to_anchor=(0.5, 1.15), shadow=True, ncol=2)

ax2.set_title("B", loc='left')
ax2.set_xscale("log")
ax2.set_ylim(bottom=0)
ax2.set_xlabel("Target false positive rate")
ax2.set_ylabel("Filter size [MiB]")
ax2.grid(color="0.9", linestyle='--', linewidth=1)
lines1, labels1 = ax2.get_legend_handles_labels()
lines2, labels2 = ax2b.get_legend_handles_labels()
ax2b.legend(lines1 + lines2, labels1 + labels2, loc='upper center', fancybox=True, bbox_to_anchor=(0.5, 1.15), shadow=True, ncol=2)

plt.tight_layout()
plt.savefig("bloom_fpr.svg")
