"""
Real-World Live Price Publisher Test

Simulates actual trading bot usage:
- 20-25 price updates per call
- Called 20 times per second (as prices update)
- Tests if it can handle within 0.5 seconds
"""

import asyncio
import time
import logging
from dataclasses import dataclass
from mqtt_connector_lib.redis_connector import RedisConnector, HighSpeedPublisher

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


@dataclass
class LivePriceTestConfig:
    """Configuration matching real trading bot usage"""
    # Redis connection
    redis_host: str = "192.168.29.42"  # Your Raspberry Pi (change to "localhost" in production)
    redis_port: int = 6379
    redis_password: str = "SuperDuperRedis6748@"

    # Real-world parameters
    duration_seconds: int = 30
    prices_per_update: int = 25  # 20-25 pairs per exchange update
    updates_per_second: int = 20  # How often prices change

    # Publisher settings
    max_concurrent_tasks: int = 10  # Enough for 20 calls/sec

    @property
    def total_messages_per_second(self) -> int:
        return self.prices_per_update * self.updates_per_second

    @property
    def time_budget_per_update(self) -> float:
        """Max time allowed per update to stay under 0.5s"""
        return 0.5 / self.updates_per_second  # 0.5s / 20 = 0.025s = 25ms


class LivePriceTestRunner:
    """Simulates real trading bot price publishing"""

    def __init__(self, config: LivePriceTestConfig):
        self.config = config
        self.connector = None
        self.publisher = None
        self.total_sent = 0
        self.total_calls = 0
        self.max_call_time = 0
        self.min_call_time = float('inf')
        self.total_call_time = 0
        self.slow_calls = 0  # Calls that took > 25ms
        self.start_time = None
        self.end_time = None

    async def setup(self):
        """Initialize Redis connector and publisher"""
        logger.info("🚀 Setting up LIVE PRICE test...")
        logger.info(f"   Simulating: {self.config.prices_per_update} prices per update")
        logger.info(f"   Update frequency: {self.config.updates_per_second} times/sec")
        logger.info(f"   Total throughput: {self.config.total_messages_per_second} msgs/sec")
        logger.info(f"   Time budget per call: {self.config.time_budget_per_update*1000:.1f}ms")

        self.connector = RedisConnector(
            host=self.config.redis_host,
            port=self.config.redis_port,
            password=self.config.redis_password,
            health_check_interval=5.0,
            circuit_breaker_threshold=3,
            circuit_breaker_timeout=30.0,
            stats_log_interval=10.0
        )

        self.publisher = HighSpeedPublisher(
            self.connector,
            max_concurrent_tasks=self.config.max_concurrent_tasks
        )

        logger.info(f"✅ Setup complete\n")

    async def generate_live_prices(self, iteration: int) -> dict:
        """Generate realistic live price data (20-25 pairs)"""
        return {
            f"PAIR_{i}": 1000.0 + iteration * 0.01 + i * 0.1
            for i in range(self.config.prices_per_update)
        }

    async def run_test(self):
        """Execute the live price test"""
        logger.info(f"{'='*60}")
        logger.info("🏁 STARTING LIVE PRICE TEST")
        logger.info(f"{'='*60}\n")

        self.start_time = time.perf_counter()
        iteration = 0

        try:
            for second in range(self.config.duration_seconds):
                second_start = time.perf_counter()
                second_sent = 0
                second_calls = 0
                second_max_time = 0

                # Simulate real trading: publish prices as they arrive
                for update in range(self.config.updates_per_second):
                    call_start = time.perf_counter()

                    # Generate and publish live prices
                    live_prices = await self.generate_live_prices(iteration)
                    success = await self.publisher.publish_prices(live_prices)

                    call_time = time.perf_counter() - call_start

                    if success:
                        second_sent += self.config.prices_per_update
                        self.total_sent += self.config.prices_per_update

                    # Track call performance
                    self.total_calls += 1
                    second_calls += 1
                    self.total_call_time += call_time
                    self.max_call_time = max(self.max_call_time, call_time)
                    self.min_call_time = min(self.min_call_time, call_time)
                    second_max_time = max(second_max_time, call_time)

                    if call_time > self.config.time_budget_per_update:
                        self.slow_calls += 1

                    iteration += 1

                # Wait for remaining time in this second
                second_duration = time.perf_counter() - second_start
                remaining_time = 1.0 - second_duration
                if remaining_time > 0:
                    await asyncio.sleep(remaining_time)

                # Print progress
                second_duration = time.perf_counter() - second_start
                health_status = "✅" if self.connector.is_healthy else "❌"
                logger.info(
                    f"Second {second + 1:2d}: "
                    f"Sent: {second_sent:4d} msgs | "
                    f"Calls: {second_calls} | "
                    f"Max call: {second_max_time*1000:.1f}ms | "
                    f"Health: {health_status}"
                )

        except KeyboardInterrupt:
            logger.warning("\n⚠️ Test interrupted by user")
        except Exception as e:
            logger.error(f"\n❌ Error during test: {e}", exc_info=True)
        finally:
            self.end_time = time.perf_counter()

    async def print_results(self):
        """Print final test results"""
        duration = self.end_time - self.start_time
        avg_call_time = self.total_call_time / self.total_calls if self.total_calls > 0 else 0
        actual_throughput = self.total_sent / duration

        logger.info(f"\n{'='*60}")
        logger.info("📊 LIVE PRICE TEST RESULTS")
        logger.info(f"{'='*60}")
        logger.info(f"Total Duration:        {duration:.2f} seconds")
        logger.info(f"Total Messages:        {self.total_sent:,}")
        logger.info(f"Total Calls:           {self.total_calls:,}")
        logger.info(f"Messages per Call:     {self.config.prices_per_update}")
        logger.info(f"Actual Throughput:     {actual_throughput:.2f} msgs/sec")
        logger.info(f"Target Throughput:     {self.config.total_messages_per_second} msgs/sec")

        logger.info(f"\n⏱️ Call Performance:")
        logger.info(f"Average call time:     {avg_call_time*1000:.2f}ms")
        logger.info(f"Min call time:         {self.min_call_time*1000:.2f}ms")
        logger.info(f"Max call time:         {self.max_call_time*1000:.2f}ms")
        logger.info(f"Time budget:           {self.config.time_budget_per_update*1000:.1f}ms")

        # Performance assessment
        slow_call_percent = (self.slow_calls / self.total_calls * 100) if self.total_calls > 0 else 0
        logger.info(f"Slow calls (>{self.config.time_budget_per_update*1000:.0f}ms): {self.slow_calls} ({slow_call_percent:.1f}%)")

        # Time for 20 calls (simulating 1 second of live updates)
        time_for_20_calls = avg_call_time * 20
        logger.info(f"\n⏱️ Time for 20 calls:    {time_for_20_calls*1000:.2f}ms ({time_for_20_calls:.3f}s)")

        if time_for_20_calls <= 0.5:
            logger.info(f"✅ TARGET ACHIEVED! (≤0.5s for 20 calls)")
        elif time_for_20_calls <= 0.6:
            logger.info(f"⚠️ Close to target (slightly above 0.5s)")
        else:
            logger.info(f"❌ Above target (>0.5s for 20 calls)")

        # Overall assessment
        if avg_call_time <= self.config.time_budget_per_update:
            logger.info(f"\nAssessment:            🎉 Excellent - All calls within budget!")
        elif slow_call_percent <= 5:
            logger.info(f"\nAssessment:            ✅ Good - 95%+ calls within budget")
        elif slow_call_percent <= 20:
            logger.info(f"\nAssessment:            ⚠️ Fair - Some calls exceed budget")
        else:
            logger.info(f"\nAssessment:            ❌ Poor - Many calls exceed budget")

        logger.info(f"{'='*60}\n")

    async def cleanup(self):
        """Clean up resources"""
        logger.info("Cleaning up...")
        if self.connector:
            await self.connector.close()
        logger.info("✅ Cleanup complete")


async def main():
    """Main entry point for live price test"""
    # Create configuration matching your real use case
    config = LivePriceTestConfig(
        redis_host="192.168.29.42",  # Your Raspberry Pi (change to "localhost" in production)
        prices_per_update=25,        # Your actual: 20-25 messages per call
        updates_per_second=20,       # Your actual: 20 calls per second
        duration_seconds=30
    )

    # Run test
    runner = LivePriceTestRunner(config)

    try:
        await runner.setup()
        await runner.run_test()
        await runner.print_results()
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    print("\n" + "="*60)
    print("🎯 LIVE PRICE PUBLISHER TEST")
    print("="*60)
    print("Simulating real trading bot behavior:")
    print("  - 20-25 messages per publish_prices() call")
    print("  - 20 calls per second")
    print("  - Target: Complete within 0.5 seconds")
    print("="*60 + "\n")

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⚠️ Test terminated by user")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)

