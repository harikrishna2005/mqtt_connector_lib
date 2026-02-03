"""
View metrics using simple logging - no Prometheus, no Grafana, no CSV.

Just prints metrics to console in a readable format.
"""

import asyncio
import time
from datetime import datetime
from smart_scaling_executor import SmartScalingExecutor
from load_generator import LoadGenerator


def simple_metrics_logger(**metrics):
    """
    Simple callback that logs metrics to console.

    No dependencies, no files, just console output.
    """
    timestamp = datetime.now().strftime("%H:%M:%S")

    workers = metrics.get('workers', 0)
    qsize = metrics.get('qsize', 0)
    qusage = metrics.get('qusage', 0) * 100
    cpu = metrics.get('cpu', 0)
    mem = metrics.get('mem', 0)

    # Create visual bars
    worker_bar = "█" * workers + "░" * (15 - workers)
    queue_bar = "█" * min(int(qusage / 5), 20) + "░" * (20 - min(int(qusage / 5), 20))

    # Print single line (compact)
    print(
        f"[{timestamp}] "
        f"Workers: {workers:2d} [{worker_bar}] | "
        f"Queue: {qsize:4d} ({qusage:5.1f}%) [{queue_bar}] | "
        f"CPU: {cpu:5.1f}% | "
        f"Mem: {mem:5.1f}%"
    )


def detailed_metrics_logger(**metrics):
    """
    Detailed callback that logs metrics with more info.

    Shows historical comparison and alerts.
    """
    timestamp = datetime.now().strftime("%H:%M:%S")

    workers = metrics.get('workers', 0)
    qsize = metrics.get('qsize', 0)
    qusage = metrics.get('qusage', 0) * 100
    cpu = metrics.get('cpu', 0)
    mem = metrics.get('mem', 0)

    # Determine status
    if qusage > 80:
        status = "🔴 HIGH"
    elif qusage > 50:
        status = "🟡 MEDIUM"
    else:
        status = "🟢 LOW"

    # Print detailed block
    print("\n" + "=" * 70)
    print(f"⏰ Timestamp: {timestamp}")
    print(f"⚙️  Workers:   {workers:2d} / 15 max")
    print(f"📦 Queue:     {qsize:4d} items ({qusage:5.1f}% usage) {status}")
    print(f"🖥️  CPU:       {cpu:5.1f}%")
    print(f"💾 Memory:    {mem:5.1f}%")

    # Alerts
    if qusage > 90:
        print("⚠️  WARNING: Queue is almost full!")
    if workers >= 14:
        print("⚠️  WARNING: Near maximum workers!")

    print("=" * 70)


async def main():
    """
    Example: Use SmartScalingExecutor with simple logging.

    Choose your style:
    - simple_metrics_logger: One line per update (compact)
    - detailed_metrics_logger: Detailed block per update (verbose)
    """

    print("🚀 SmartScalingExecutor - Simple Metrics Logging")
    print("=" * 70)
    print()
    print("Choose your logging style by changing the metrics_cb parameter:")
    print("  - simple_metrics_logger: Compact, one-line updates")
    print("  - detailed_metrics_logger: Detailed blocks with alerts")
    print()
    print("=" * 70)
    print()

    # Create executor with simple logging
    executor = SmartScalingExecutor(
        min_workers=5,
        max_workers=15,
        queue_check_interval=2.0,
        metrics_cb=simple_metrics_logger  # Or use detailed_metrics_logger
    )

    # Create load generator
    load_gen = LoadGenerator(
        executor=executor,
        rate=100,
        burst_interval=5,
        burst_sizes=[300, 500, 700]
    )

    # Start
    await executor.start()
    await load_gen.start()

    print("📊 Metrics updating every 2 seconds...")
    print("Press Ctrl+C to stop")
    print()

    try:
        while True:
            await asyncio.sleep(60)
    except KeyboardInterrupt:
        print("\n\n⏹️  Stopping...")
    finally:
        await load_gen.stop()
        await executor.stop()
        print("✅ Stopped")


if __name__ == "__main__":
    asyncio.run(main())

