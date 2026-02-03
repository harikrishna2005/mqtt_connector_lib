import csv
import time
from pathlib import Path


class MetricsCollector:
    """
    Collects metrics from SmartScalingExecutor into a CSV file.
    """

    def __init__(self, filepath="metrics.csv"):
        self.filepath = Path(filepath)
        self._ensure_header()

    def _ensure_header(self):
        if not self.filepath.exists():
            with open(self.filepath, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
                        "timestamp",
                        "cpu_ewma",
                        "memory",
                        "queue_size",
                        "queue_usage",
                        "workers",
                    ]
                )

    def record(self, cpu, mem, qsize, qusage, workers):
        with open(self.filepath, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    int(time.time()),
                    cpu,
                    mem,
                    qsize,
                    qusage,
                    workers,
                ]
            )
