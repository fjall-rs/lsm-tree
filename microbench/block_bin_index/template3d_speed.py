from pathlib import Path
import json
import matplotlib.pyplot as plt

# Read JSONL file using Path API
data_file = Path("data.jsonl")
lines = [line for line in data_file.read_text().splitlines() if line.strip()]
data_points = [json.loads(line) for line in lines]
filtered_data = [point for point in data_points if not point.get("unsafe", False)]

# Extract the axes
x_vals = [point["item_count"] for point in filtered_data]
y_vals = [point["restart_interval"] for point in filtered_data]
z_vals = [point["rps_ns"] for point in filtered_data]

# Plotting
fig = plt.figure(figsize=(6, 4))
ax = fig.add_subplot(111, projection='3d')

trisurf = ax.plot_trisurf(x_vals, y_vals, z_vals, cmap='viridis', edgecolor='none', alpha=0.8)

cbar = fig.colorbar(trisurf, ax=ax, pad=0.1, shrink=0.8, aspect=15)
cbar.set_label("", labelpad=10)

ax.set_xlabel("# KV tuples")
ax.set_ylabel("Restart interval")
ax.set_zlabel("Read latency [ns]")

ax.set_zlim(bottom=0)

ax.invert_xaxis()

fig.subplots_adjust(left=-0.3, right=0.99, top=0.99, bottom=0.08)

# plt.tight_layout()
plt.savefig("binary_index_3d_speed.svg")
