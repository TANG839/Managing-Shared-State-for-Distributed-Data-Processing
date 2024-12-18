import matplotlib.pyplot as plt
import re

# Data parsing function
def parse_elapsed_time(time_str):
    minutes, seconds = map(float, time_str.split(':'))
    return minutes * 60 + seconds

# Raw data
# data = """
# 1 worker: 2:53.20
# 2 workers: 1:45.06
# 3 workers: 1:35.55
# 4 workers: 1:34.70
# 5 workers: 1:24.71
# 6 workers: 1:29.30
# """

data = """
2 partitions: 1:22.16
4 partitions: 1:29.65
8 partitions: 1:23.28
16 partitions: 1:24.71
32 partitions: 1:43.25
100 partitions: 3:39.59
"""

# Process data
workers = []
elapsed_times = []

for line in data.strip().split('\n'):
    num_workers = int(line.split()[0])
    time_str = line.split(': ')[1]
    elapsed_time = parse_elapsed_time(time_str)
    
    workers.append(num_workers)
    elapsed_times.append(elapsed_time)

# Create the plot
plt.figure(figsize=(10, 6))
plt.plot(workers, elapsed_times, 'bo-', linewidth=2, markersize=8)

# Customize the plot
plt.title('Task Completion Time vs Number of Partitions for 5 workers', fontsize=14, pad=20)
plt.xlabel('Number of Partitions', fontsize=12)
plt.ylabel('Elapsed Time (seconds)', fontsize=12)
plt.grid(True, linestyle='--', alpha=0.7)

# Add value labels on each point
for i, (x, y) in enumerate(zip(workers, elapsed_times)):
    plt.annotate(f'{y:.1f}s', 
                (x, y), 
                textcoords="offset points", 
                xytext=(0,10), 
                ha='center')

plt.xticks(workers)

plt.tight_layout()

plt.savefig('partition_performance.png')
plt.show()