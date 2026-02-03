import csv
import matplotlib.pyplot as plt


def load_metrics(path="metrics.csv"):
    ts = []
    cpu = []
    mem = []
    qsize = []
    qusage = []
    workers = []

    with open(path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts.append(int(row["timestamp"]))
            cpu.append(float(row["cpu_ewma"]))
            mem.append(float(row["memory"]))
            qsize.append(int(row["queue_size"]))
            qusage.append(float(row["queue_usage"]))
            workers.append(int(row["workers"]))

    return ts, cpu, mem, qsize, qusage, workers


def plot_and_save(x, y, title, ylabel, filename):
    plt.figure(figsize=(10, 4))
    plt.plot(x, y)
    plt.title(title)
    plt.xlabel("Time")
    plt.ylabel(ylabel)
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()
    print(f"[PLOT] Saved {filename}")


def main():
    ts, cpu, mem, qsize, qusage, workers = load_metrics()

    plot_and_save(ts, cpu, "CPU EWMA Over Time", "CPU %", "cpu_usage.png")
    plot_and_save(ts, mem, "Memory Usage Over Time", "Memory %", "memory_usage.png")
    plot_and_save(ts, qsize, "Queue Size Over Time", "Queue Size", "queue_usage.png")
    plot_and_save(ts, workers, "Workers Over Time", "Worker Count", "worker_count.png")


if __name__ == "__main__":
    main()
