import asyncio
from smart_executor import SmartScalingExecutor
from metrics_collector import MetricsCollector
from load_generator import LoadGenerator
from dashboard import Dashboard


async def main():
    # Metrics Collector
    metrics = MetricsCollector(filepath="metrics.csv")

    # Dashboard
    dashboard = Dashboard()

    # Smart Executor
    executor = SmartScalingExecutor(
        min_workers=3,
        max_workers=20,
        queue_size=1000,
        scaler_interval=2.0,
        ewma_alpha=0.3,
        metrics_cb=lambda **m: (
            metrics.record(
                cpu=m["cpu"],
                mem=m["mem"],
                qsize=m["qsize"],
                qusage=m["qusage"],
                workers=m["workers"],
            ),
            dashboard.update_metrics(
                cpu=m["cpu"],
                mem=m["mem"],
                qsize=m["qsize"],
                qusage=m["qusage"],
                workers=m["workers"],
            ),
        ),
    )

    # Start dashboard in background
    asyncio.create_task(dashboard.run())

    # Start executor
    await executor.start()

    # Load generator
    load = LoadGenerator(executor, rate=200)
    asyncio.create_task(load.start())

    # Run forever
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\n[DEMO] Stopping demo...")
        await load.stop()
        await executor.stop()


if __name__ == "__main__":
    asyncio.run(main())
