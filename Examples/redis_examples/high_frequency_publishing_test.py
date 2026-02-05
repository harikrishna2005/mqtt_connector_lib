"""
High-Frequency Publishing Test

Simulates calling publish_prices 20-30 times per second,
with each call publishing 20-30 messages.

This tests your actual production use case.
"""

import asyncio
import time
import random
from dataclasses import dataclass
from mqtt_connector_lib.redis_connector import RedisConnector, HighSpeedPublisher


@dataclass
class HighFrequencyTestConfig:
    """Configuration for high-frequency test"""
    redis_host: str = "192.168.29.42"
    redis_port: int = 6379
    redis_password: str = "SuperDuperRedis6748@"

    # Test parameters
    duration_seconds: int = 30
    calls_per_second: int = 25  # 20-30 range, using middle value
    messages_per_call: int = 25  # 20-30 range, using middle value

    # Publisher settings
    max_concurrent_tasks: int = 30  # Allow enough concurrency

    @property
    def total_throughput(self) -> int:
        """Calculate total messages per second"""
        return self.calls_per_second * self.messages_per_call

    @property
    def interval_between_calls(self) -> float:
        """Calculate interval between calls in seconds"""
        return 1.0 / self.calls_per_second


class HighFrequencyTestRunner:
    """Test runner for high-frequency publishing"""

    def __init__(self, config: HighFrequencyTestConfig):
        self.config = config
        self.connector = None
        self.publisher = None

        # Metrics
        self.total_calls = 0
        self.successful_calls = 0
        self.failed_calls = 0
        self.total_messages_sent = 0
        self.total_messages_dropped = 0

        # Timing metrics
        self.call_times = []
        self.start_time = None
        self.end_time = None

    async def setup(self):
        """Initialize connector and publisher"""
        print("🔧 Setting up high-frequency test...")
        print(f"   Target: {self.config.calls_per_second} calls/sec")
        print(f"   Messages per call: {self.config.messages_per_call}")
        print(f"   Total throughput: {self.config.total_throughput} msgs/sec")
        print(f"   Test duration: {self.config.duration_seconds} seconds\n")

        self.connector = RedisConnector(
            host=self.config.redis_host,
            port=self.config.redis_port,
            password=self.config.redis_password,
            health_check_interval=5.0
        )

        self.publisher = HighSpeedPublisher(
            self.connector,
            max_concurrent_tasks=self.config.max_concurrent_tasks
        )

        print("✅ Setup complete\n")

    def generate_prices(self, iteration: int) -> dict:
        """Generate realistic price data"""
        num_pairs = self.config.messages_per_call
        base_prices = {
            "BTCUSDT": 42000,
            "ETHUSDT": 2500,
            "BNBUSDT": 300,
            "XRPUSDT": 0.5,
            "ADAUSDT": 0.4,
            "SOLUSDT": 100,
            "DOGEUSDT": 0.08,
            "DOTUSDT": 7,
            "MATICUSDT": 0.8,
            "LTCUSDT": 70,
        }

        # Generate pairs with realistic price variations
        pairs = list(base_prices.keys())
        result = {}

        for i in range(num_pairs):
            if i < len(pairs):
                pair = pairs[i]
                base_price = base_prices[pair]
            else:
                # Generate additional pairs if needed
                pair = f"PAIR{i}USDT"
                base_price = 100.0

            # Add random variation (±0.1%)
            variation = random.uniform(-0.001, 0.001)
            price = base_price * (1 + variation) + (iteration * 0.01)
            result[pair] = round(price, 8)

        return result

    async def run_test(self):
        """Execute the high-frequency test"""
        print("=" * 80)
        print("🚀 STARTING HIGH-FREQUENCY PUBLISHING TEST")
        print("=" * 80)
        print(f"Calling publish_prices {self.config.calls_per_second}x per second")
        print(f"Each call publishes {self.config.messages_per_call} messages")
        print(f"Target throughput: {self.config.total_throughput} msgs/sec")
        print("=" * 80)
        print()

        self.start_time = time.perf_counter()
        iteration = 0

        try:
            for second in range(self.config.duration_seconds):
                second_start = time.perf_counter()
                second_calls = 0
                second_successful = 0
                second_failed = 0
                call_times_this_second = []

                # Make multiple calls in this second
                for call_num in range(self.config.calls_per_second):
                    call_start = time.perf_counter()

                    # Generate and publish prices
                    prices = self.generate_prices(iteration)
                    success = await self.publisher.publish_prices(prices)

                    call_end = time.perf_counter()
                    call_time = (call_end - call_start) * 1000  # ms
                    call_times_this_second.append(call_time)
                    self.call_times.append(call_time)

                    # Update metrics
                    self.total_calls += 1
                    second_calls += 1

                    if success:
                        self.successful_calls += 1
                        second_successful += 1
                        self.total_messages_sent += len(prices)
                    else:
                        self.failed_calls += 1
                        second_failed += 1
                        self.total_messages_dropped += len(prices)

                    iteration += 1

                    # Small sleep to distribute calls evenly (optional)
                    # await asyncio.sleep(self.config.interval_between_calls)

                # Wait for remaining time in this second
                second_duration = time.perf_counter() - second_start
                remaining_time = 1.0 - second_duration
                if remaining_time > 0:
                    await asyncio.sleep(remaining_time)

                # Print progress
                actual_duration = time.perf_counter() - second_start
                avg_call_time = sum(call_times_this_second) / len(call_times_this_second)
                max_call_time = max(call_times_this_second)

                health = "✅" if self.connector.is_healthy else "❌"

                print(
                    f"Second {second + 1:2d}/{self.config.duration_seconds}: "
                    f"Calls: {second_calls:2d} | "
                    f"Success: {second_successful:2d} | "
                    f"Failed: {second_failed:2d} | "
                    f"Avg: {avg_call_time:5.2f}ms | "
                    f"Max: {max_call_time:5.2f}ms | "
                    f"Duration: {actual_duration:.3f}s | "
                    f"Health: {health}"
                )

                # Periodic stats
                if (second + 1) % 10 == 0:
                    stats = self.connector.get_stats()
                    print(f"   📊 Stats: Sent={stats['messages_sent']:,} | "
                          f"Dropped={stats['messages_dropped']} | "
                          f"Success={stats['success_rate']}")

        except KeyboardInterrupt:
            print("\n⚠️ Test interrupted by user")
        except Exception as e:
            print(f"\n❌ Error during test: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.end_time = time.perf_counter()

    async def print_results(self):
        """Print detailed test results"""
        duration = self.end_time - self.start_time
        stats = self.connector.get_stats()

        # Calculate statistics
        avg_call_time = sum(self.call_times) / len(self.call_times) if self.call_times else 0
        min_call_time = min(self.call_times) if self.call_times else 0
        max_call_time = max(self.call_times) if self.call_times else 0

        # Count slow calls
        slow_calls = sum(1 for t in self.call_times if t > 50)  # >50ms

        actual_throughput = self.total_messages_sent / duration if duration > 0 else 0
        actual_call_rate = self.total_calls / duration if duration > 0 else 0

        print("\n" + "=" * 80)
        print("📊 HIGH-FREQUENCY TEST RESULTS")
        print("=" * 80)
        print(f"\n⏱️  Test Duration:")
        print(f"   Total time:          {duration:.2f} seconds")
        print(f"\n📞 Call Statistics:")
        print(f"   Total calls:         {self.total_calls:,}")
        print(f"   Successful calls:    {self.successful_calls:,} ({self.successful_calls/self.total_calls*100:.1f}%)")
        print(f"   Failed calls:        {self.failed_calls:,} ({self.failed_calls/self.total_calls*100:.1f}%)")
        print(f"   Actual call rate:    {actual_call_rate:.1f} calls/sec")
        print(f"   Target call rate:    {self.config.calls_per_second} calls/sec")
        print(f"\n📨 Message Statistics:")
        print(f"   Messages sent:       {stats['messages_sent']:,}")
        print(f"   Messages dropped:    {stats['messages_dropped']:,}")
        print(f"   Messages congested:  {stats['messages_congested']:,}")
        print(f"   Success rate:        {stats['success_rate']}")
        print(f"\n🚀 Throughput:")
        print(f"   Actual throughput:   {actual_throughput:.0f} msgs/sec")
        print(f"   Target throughput:   {self.config.total_throughput} msgs/sec")
        print(f"   Efficiency:          {actual_throughput/self.config.total_throughput*100:.1f}%")
        print(f"\n⏱️  Call Latency:")
        print(f"   Average call time:   {avg_call_time:.2f}ms")
        print(f"   Min call time:       {min_call_time:.2f}ms")
        print(f"   Max call time:       {max_call_time:.2f}ms")
        print(f"   Slow calls (>50ms):  {slow_calls} ({slow_calls/len(self.call_times)*100:.1f}%)")

        # Assessment
        print(f"\n🎯 Assessment:")

        if actual_throughput >= self.config.total_throughput * 0.95:
            print(f"   🎉 EXCELLENT - Achieved {actual_throughput/self.config.total_throughput*100:.1f}% of target!")
        elif actual_throughput >= self.config.total_throughput * 0.85:
            print(f"   ✅ GOOD - Achieved {actual_throughput/self.config.total_throughput*100:.1f}% of target")
        elif actual_throughput >= self.config.total_throughput * 0.70:
            print(f"   ⚠️ FAIR - Achieved {actual_throughput/self.config.total_throughput*100:.1f}% of target")
        else:
            print(f"   ❌ POOR - Only achieved {actual_throughput/self.config.total_throughput*100:.1f}% of target")

        if avg_call_time < 20:
            print(f"   ✅ Call latency is excellent (<20ms avg)")
        elif avg_call_time < 50:
            print(f"   ✅ Call latency is good (<50ms avg)")
        else:
            print(f"   ⚠️ Call latency is high (>50ms avg)")

        if self.failed_calls == 0:
            print(f"   ✅ Zero failures - perfect reliability!")
        elif self.failed_calls / self.total_calls < 0.01:
            print(f"   ✅ Very low failure rate (<1%)")
        else:
            print(f"   ⚠️ Failure rate: {self.failed_calls/self.total_calls*100:.1f}%")

        print("=" * 80)
        print()

    async def cleanup(self):
        """Clean up resources"""
        print("🧹 Cleaning up...")
        if self.connector:
            await self.connector.close()
        print("✅ Cleanup complete")


async def main():
    """Main entry point"""
    # Test with your actual requirements
    config = HighFrequencyTestConfig(
        duration_seconds=30,
        calls_per_second=25,      # 20-30 range, using 25
        messages_per_call=25,      # 20-30 range, using 25
        max_concurrent_tasks=30    # Allow enough concurrency
    )

    runner = HighFrequencyTestRunner(config)

    try:
        await runner.setup()
        await runner.run_test()
        await runner.print_results()
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.WARNING,  # Reduce noise
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⚠️ Test terminated by user")

