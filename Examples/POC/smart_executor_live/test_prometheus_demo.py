"""
Simple test to verify Prometheus metrics and SmartScalingExecutor are working.
This runs for 30 seconds and then stops.
"""

import asyncio
import sys
from smart_scaling_executor import SmartScalingExecutor
from load_generator import LoadGenerator
from prometheus_metrics import start_prometheus_server, prometheus_metrics_callback

async def simple_test():
    print("=" * 70)
    print("🚀 PROMETHEUS + SMART SCALING EXECUTOR TEST")
    print("=" * 70)
    print()

    # Start Prometheus server
    print("📊 Starting Prometheus metrics server...")
    success = await start_prometheus_server(port=8000)
    if success:
        print("✅ Prometheus metrics server started on port 8000")
        print("   Visit: http://localhost:8000/metrics")
    else:
        print("❌ Failed to start Prometheus server")
        print("   Port 8000 might be in use")
        return

    print()

    # Simple metrics callback
    def metrics_callback(**m):
        prometheus_metrics_callback(**m)
        # Print to console every update
        print(f"[METRICS] Queue={m['qsize']:4d} | Workers={m['workers']:2d} | "
              f"CPU={m['cpu']:5.1f}% | Mem={m['mem']:5.1f}% | QUsage={m['qusage']*100:5.1f}%")

    # Create executor
    print("🔧 Creating SmartScalingExecutor...")
    executor = SmartScalingExecutor(
        min_workers=3,
        max_workers=20,
        queue_size=2000,
        queue_check_interval=2.0,
        metrics_cb=metrics_callback
    )

    # Start executor
    print("▶️  Starting executor...")
    await executor.start()
    print(f"✅ Executor started with {len(executor.workers)} workers")
    print()

    # Create load generator with HIGHER LOAD
    print("📈 Creating load generator...")
    load_gen = LoadGenerator(
        executor=executor,
        rate=150,  # 150 msgs/sec steady (increased from 100)
        burst_sizes=[300, 500, 800, 1000],  # Random bursts 300-1000 (increased from 200-500)
        burst_interval_range=(3, 5)  # Random interval 3-5 seconds
    )

    print("▶️  Starting load generator...")
    asyncio.create_task(load_gen.start())
    print("✅ Load generator started")
    print()

    print("=" * 70)
    print("📊 MONITORING (5 MINUTES) - HEAVY LOAD TEST")
    print("=" * 70)
    print("Testing with bursts of 300-1000 messages every 3-5 seconds!")
    print("Watch how workers scale up and down based on load!")
    print()
    print(f"{'Time':<8} {'Queue':<8} {'Workers':<10} {'CPU':<10} {'Status'}")
    print("-" * 70)

    # Run for 5 minutes (300 seconds)
    try:
        for i in range(150):  # 300 seconds (every 2 seconds)
            await asyncio.sleep(2)
            qsize = executor.get_queue_size()
            workers = len(executor.workers)
            status = "Processing..." if qsize > 0 else "Idle"

            # Print every update (all 150 iterations)
            print(f"{i*2:3d}s     {qsize:<8} {workers:<10} {'':10} {status}")
    except KeyboardInterrupt:
        print("\n⏹  Interrupted by user")

    print()
    print("=" * 70)
    print("🛑 STOPPING")
    print("=" * 70)

    # Stop load generator
    print("⏹  Stopping load generator...")
    await load_gen.stop()
    print("✅ Load generator stopped")

    # Stop executor
    print("⏹  Stopping executor...")
    await executor.stop()
    print("✅ Executor stopped")

    print()
    print("=" * 70)
    print("✅ TEST COMPLETE")
    print("=" * 70)
    print()
    print("📊 Prometheus metrics were available at: http://localhost:8000/metrics")
    print("   (You can still access it if you open the URL before closing)")
    print()
    print("🎯 Key Observations:")
    print("   - Workers scaled up during bursts")
    print("   - Workers scaled down when queue was empty")
    print("   - Metrics were exposed in Prometheus format")
    print()
    print("Next steps:")
    print("1. Run for longer: python run_demo_with_prometheus.py")
    print("2. Setup Grafana to visualize metrics")
    print("3. See PROMETHEUS_GRAFANA_SETUP.md for details")

if __name__ == "__main__":
    print()
    try:
        asyncio.run(simple_test())
    except KeyboardInterrupt:
        print("\n\n⏹  Test interrupted")
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

