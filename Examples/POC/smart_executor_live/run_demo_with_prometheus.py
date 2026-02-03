"""
Example: Run SmartScalingExecutor with Prometheus metrics support.

This demo:
1. Starts Prometheus HTTP server on port 8000
2. Runs the executor with metrics exported to Prometheus
3. Continues to collect CSV metrics for local analysis
4. Shows live terminal dashboard

To view metrics:
- Terminal Dashboard: See in console
- CSV: metrics.csv file
- Prometheus: http://localhost:8000/metrics
- Grafana: Configure Prometheus as data source, create dashboard

Prometheus Configuration (prometheus.yml):
```yaml
scrape_configs:
  - job_name: 'smart_executor'
    scrape_interval: 5s
    static_configs:
      - targets: ['localhost:8000']
```

Then run: prometheus --config.file=prometheus.yml
Then setup Grafana: http://localhost:3000 (default)
"""

import asyncio
from smart_scaling_executor import SmartScalingExecutor
from metrics_collector import MetricsCollector
from load_generator import LoadGenerator
from dashboard import Dashboard
from prometheus_metrics import start_prometheus_server, prometheus_metrics_callback


async def main():
    # Start Prometheus HTTP server for metrics scraping
    print("Starting Prometheus metrics server...")
    await start_prometheus_server(port=8000)
    print("✅ Prometheus metrics available at: http://localhost:8000/metrics\n")

    # Metrics Collector (CSV)
    metrics = MetricsCollector(filepath="metrics.csv")

    # Dashboard (Terminal)
    dashboard = Dashboard()

    # Combined callback: Updates CSV, Terminal Dashboard, AND Prometheus
    def combined_callback(**m):
        # Update CSV
        metrics.record(
            cpu=m["cpu"],
            mem=m["mem"],
            qsize=m["qsize"],
            qusage=m["qusage"],
            workers=m["workers"],
        )
        # Update Terminal Dashboard
        dashboard.update_metrics(
            cpu=m["cpu"],
            mem=m["mem"],
            qsize=m["qsize"],
            qusage=m["qusage"],
            workers=m["workers"],
        )
        # Update Prometheus Metrics
        prometheus_metrics_callback(
            cpu=m["cpu"],
            mem=m["mem"],
            qsize=m["qsize"],
            qusage=m["qusage"],
            workers=m["workers"],
        )

    # Smart Executor with Prometheus support
    executor = SmartScalingExecutor(
        min_workers=3,
        max_workers=30,
        queue_size=2000,
        queue_check_interval=2.0,
        ewma_alpha=0.2,
        metrics_cb=combined_callback,
    )

    # Start dashboard in background
    asyncio.create_task(dashboard.run())

    # Start executor
    await executor.start()

    # Load generator - randomly submits bursts of 200 or 500 messages every 5 seconds
    load = LoadGenerator(
        executor,
        rate=200,  # Steady state: 200 msgs/sec
        burst_sizes=[200, 500],  # Random bursts: 200 or 500 messages
        burst_interval=5  # Every 5 seconds
    )
    asyncio.create_task(load.start())

    print("\n" + "="*70)
    print("🚀 Smart Scaling Executor Demo Running")
    print("="*70)
    print("📊 Metrics available at:")
    print("   - Terminal Dashboard: See below")
    print("   - CSV File: metrics.csv")
    print("   - Prometheus: http://localhost:8000/metrics")
    print("\n💡 Next Steps:")
    print("   1. Setup Prometheus to scrape: http://localhost:8000/metrics")
    print("   2. Add Prometheus as data source in Grafana")
    print("   3. Create Grafana dashboard with these metrics:")
    print("      - smart_executor_queue_size")
    print("      - smart_executor_queue_usage_percent")
    print("      - smart_executor_worker_count")
    print("      - smart_executor_cpu_ewma_percent")
    print("      - smart_executor_memory_usage_percent")
    print("\n⏹  Press Ctrl+C to stop...\n")
    print("="*70 + "\n")

    # Run forever
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\n[DEMO] Stopping demo...")
        await load.stop()
        await executor.stop()
        print("✅ Demo stopped gracefully")


if __name__ == "__main__":
    asyncio.run(main())

