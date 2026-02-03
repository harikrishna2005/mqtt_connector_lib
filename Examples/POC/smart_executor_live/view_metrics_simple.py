"""
Simple script to view SmartScalingExecutor metrics without Grafana.

Uses CSV export + terminal dashboard for real-time monitoring.
"""

import asyncio
import time
from smart_scaling_executor import SmartScalingExecutor
from load_generator import LoadGenerator
from metrics_collector import MetricsCollector


class SimpleDashboard:
    """Simple terminal-based dashboard to view metrics"""

    def __init__(self):
        self.metrics_history = []
        self.max_history = 30  # Keep last 30 data points

    def display(self, **metrics):
        """Display metrics in a simple terminal dashboard"""
        # Store in history
        self.metrics_history.append(metrics)
        if len(self.metrics_history) > self.max_history:
            self.metrics_history.pop(0)

        # Clear screen (works on Windows and Unix)
        print("\033[2J\033[H", end="")

        # Header
        print("=" * 80)
        print("📊 SmartScalingExecutor - Live Metrics Dashboard")
        print("=" * 80)
        print()

        # Current metrics
        workers = metrics.get('workers', 0)
        qsize = metrics.get('qsize', 0)
        qusage = metrics.get('qusage', 0)
        cpu = metrics.get('cpu', 0)
        mem = metrics.get('mem', 0)

        print(f"⚙️  Workers:     {workers:3d}   {'█' * workers}{' ' * (20 - workers)}")
        print(f"📦 Queue Size:  {qsize:4d}  {'█' * min(qsize // 50, 40)}")
        print(f"📊 Queue Usage: {qusage*100:5.1f}%")
        print(f"🖥️  CPU:        {cpu:5.1f}%  {'█' * int(cpu / 5)}")
        print(f"💾 Memory:     {mem:5.1f}%  {'█' * int(mem / 5)}")
        print()

        # Trend (last 30 data points)
        if len(self.metrics_history) > 1:
            print("📈 Worker Trend (last 30 intervals):")
            self._print_sparkline([m.get('workers', 0) for m in self.metrics_history], max_value=20)

            print("\n📈 Queue Trend (last 30 intervals):")
            self._print_sparkline([m.get('qsize', 0) for m in self.metrics_history], max_value=500)

        print()
        print("=" * 80)
        print(f"⏱️  Last updated: {time.strftime('%H:%M:%S')}")
        print("Press Ctrl+C to stop")
        print("=" * 80)

    def _print_sparkline(self, data, max_value):
        """Print a simple ASCII sparkline chart"""
        if not data:
            return

        # Normalize to 0-10 range
        normalized = [min(10, int(val / max_value * 10)) for val in data]

        # Print bars
        for level in range(10, -1, -1):
            line = ""
            for val in normalized:
                if val >= level:
                    line += "█"
                else:
                    line += " "
            if level % 2 == 0:  # Add scale markers
                print(f"{int(level * max_value / 10):4d} | {line}")
            else:
                print(f"     | {line}")


async def main():
    """Run executor with simple dashboard"""

    # Create dashboard
    dashboard = SimpleDashboard()

    # Create metrics collector (for CSV export)
    collector = MetricsCollector(filename="metrics.csv")

    # Combined callback: dashboard + CSV
    def combined_callback(**metrics):
        dashboard.display(**metrics)
        collector.collect(**metrics)

    # Create executor
    executor = SmartScalingExecutor(
        min_workers=5,
        max_workers=15,
        queue_check_interval=2.0,
        metrics_cb=combined_callback
    )

    # Create load generator
    load_gen = LoadGenerator(
        executor=executor,
        rate=100,  # Base rate: 100 msgs/sec
        burst_interval=5,  # Burst every 5 seconds
        burst_sizes=[300, 500, 700]  # Random burst sizes
    )

    # Start everything
    await executor.start()
    await load_gen.start()

    print("🚀 Starting Simple Dashboard...")
    print("📊 Metrics will update every 2 seconds")
    print("💾 Data saved to: metrics.csv")
    print()

    try:
        # Run indefinitely
        while True:
            await asyncio.sleep(60)
    except KeyboardInterrupt:
        print("\n\n⏹️  Stopping...")
    finally:
        # Cleanup
        await load_gen.stop()
        await executor.stop()
        collector.save()

        print("\n✅ Stopped. Metrics saved to: metrics.csv")
        print("📊 View with: python -c \"import pandas as pd; print(pd.read_csv('metrics.csv'))\"")


if __name__ == "__main__":
    asyncio.run(main())

