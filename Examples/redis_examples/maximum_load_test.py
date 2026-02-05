"""
Maximum Load Test - Push to the Limits

Tests the absolute maximum: 30 calls/sec × 30 messages/call = 900 msgs/sec
"""

import asyncio
import time
import random
from mqtt_connector_lib.redis_connector import RedisConnector, HighSpeedPublisher


async def maximum_load_test():
    """Test maximum load capacity"""

    print("\n" + "=" * 80)
    print("🔥 MAXIMUM LOAD TEST")
    print("=" * 80)
    print("Testing absolute maximum load:")
    print("  - 30 calls per second")
    print("  - 30 messages per call")
    print("  - Total: 900 msgs/sec")
    print("=" * 80)
    print()

    # Setup
    connector = RedisConnector(
        host="192.168.29.42",
        port=6379,
        password="SuperDuperRedis6748@"
    )

    publisher = HighSpeedPublisher(connector, max_concurrent_tasks=50)  # Increased

    # Test parameters
    duration = 30
    calls_per_second = 30
    messages_per_call = 30

    # Metrics
    total_calls = 0
    successful_calls = 0
    failed_calls = 0
    call_times = []

    start_time = time.perf_counter()

    try:
        for second in range(duration):
            second_start = time.perf_counter()
            second_successful = 0
            second_failed = 0
            second_call_times = []

            # Make 30 calls in this second
            for call in range(calls_per_second):
                # Generate prices
                prices = {
                    f"PAIR{i}": 1000.0 + random.random()
                    for i in range(messages_per_call)
                }

                call_start = time.perf_counter()
                success = await publisher.publish_prices(prices)
                call_end = time.perf_counter()

                call_time = (call_end - call_start) * 1000
                call_times.append(call_time)
                second_call_times.append(call_time)

                total_calls += 1
                if success:
                    successful_calls += 1
                    second_successful += 1
                else:
                    failed_calls += 1
                    second_failed += 1

            # Wait for remaining time
            second_duration = time.perf_counter() - second_start
            remaining = 1.0 - second_duration
            if remaining > 0:
                await asyncio.sleep(remaining)

            # Print progress
            actual_duration = time.perf_counter() - second_start
            avg_time = sum(second_call_times) / len(second_call_times)
            max_time = max(second_call_times)

            health = "✅" if connector.is_healthy else "❌"

            print(
                f"Second {second + 1:2d}/{duration}: "
                f"Calls: {calls_per_second} | "
                f"Success: {second_successful:2d} | "
                f"Failed: {second_failed:2d} | "
                f"Avg: {avg_time:6.2f}ms | "
                f"Max: {max_time:6.2f}ms | "
                f"Duration: {actual_duration:.3f}s | "
                f"Health: {health}"
            )

            # Stats every 10 seconds
            if (second + 1) % 10 == 0:
                stats = connector.get_stats()
                print(f"   📊 Stats: Sent={stats['messages_sent']:,} | "
                      f"Dropped={stats['messages_dropped']} | "
                      f"Success={stats['success_rate']}")

    finally:
        end_time = time.perf_counter()
        duration_actual = end_time - start_time

        # Final stats
        stats = connector.get_stats()
        avg_call_time = sum(call_times) / len(call_times) if call_times else 0
        actual_throughput = stats['messages_sent'] / duration_actual

        print("\n" + "=" * 80)
        print("📊 MAXIMUM LOAD TEST RESULTS")
        print("=" * 80)
        print(f"Duration:            {duration_actual:.2f} seconds")
        print(f"Total calls:         {total_calls:,}")
        print(f"Successful calls:    {successful_calls:,} ({successful_calls/total_calls*100:.1f}%)")
        print(f"Failed calls:        {failed_calls:,} ({failed_calls/total_calls*100:.1f}%)")
        print(f"Average call time:   {avg_call_time:.2f}ms")
        print(f"Messages sent:       {stats['messages_sent']:,}")
        print(f"Messages dropped:    {stats['messages_dropped']:,}")
        print(f"Actual throughput:   {actual_throughput:.0f} msgs/sec")
        print(f"Target throughput:   900 msgs/sec")
        print(f"Efficiency:          {actual_throughput/900*100:.1f}%")
        print("=" * 80)

        if actual_throughput >= 850:
            print("🎉 EXCELLENT - Handles maximum load!")
        elif actual_throughput >= 750:
            print("✅ GOOD - Handles most of maximum load")
        elif actual_throughput >= 650:
            print("⚠️ FAIR - Can handle with some optimization")
        else:
            print("❌ Need optimization for maximum load")

        print()
        await connector.close()


async def comparison_test():
    """Run tests at different loads for comparison"""

    print("\n" + "=" * 80)
    print("📊 LOAD COMPARISON TEST")
    print("=" * 80)
    print()

    test_configs = [
        (20, 20, "Minimum Load"),
        (25, 25, "Average Load"),
        (30, 30, "Maximum Load"),
    ]

    results = []

    for calls_per_sec, msgs_per_call, label in test_configs:
        print(f"Testing {label}: {calls_per_sec} calls/sec × {msgs_per_call} msgs...")

        connector = RedisConnector(
            host="192.168.29.42",
            port=6379,
            password="SuperDuperRedis6748@"
        )

        publisher = HighSpeedPublisher(connector, max_concurrent_tasks=50)

        total_throughput = calls_per_sec * msgs_per_call
        duration = 10  # 10 seconds each

        start_time = time.perf_counter()

        for second in range(duration):
            for call in range(calls_per_sec):
                prices = {f"P{i}": 1000.0 + i for i in range(msgs_per_call)}
                await publisher.publish_prices(prices)
            await asyncio.sleep(1.0 / calls_per_sec)  # Pace the calls

        end_time = time.perf_counter()
        actual_duration = end_time - start_time

        stats = connector.get_stats()
        actual_throughput = stats['messages_sent'] / actual_duration
        efficiency = (actual_throughput / total_throughput) * 100

        results.append((label, total_throughput, actual_throughput, efficiency))

        print(f"  ✅ {label}: {actual_throughput:.0f} msgs/sec ({efficiency:.1f}%)\n")

        await connector.close()

    # Print comparison
    print("=" * 80)
    print("📊 COMPARISON SUMMARY")
    print("=" * 80)
    for label, target, actual, efficiency in results:
        status = "✅" if efficiency >= 90 else "⚠️"
        print(f"{status} {label:15s}: {actual:6.0f} msgs/sec ({efficiency:5.1f}%) | Target: {target}")
    print("=" * 80)
    print()


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.WARNING)

    print("\n🚀 Running Maximum Load Tests...\n")

    # Run maximum load test
    asyncio.run(maximum_load_test())

    # Run comparison
    # asyncio.run(comparison_test())  # Uncomment to run comparison

