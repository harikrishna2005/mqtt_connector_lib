"""
Optimized Stress Test for Production Raspberry Pi Setup

Target: Achieve 0.5 seconds for 1000 messages (2000 msgs/sec)
Environment: Both publisher and Redis on same Raspberry Pi (localhost)
"""

import asyncio
import time
import logging
from dataclasses import dataclass
from mqtt_connector_lib.redis_connector import RedisConnector, HighSpeedPublisher

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


@dataclass
class ProductionStressTestConfig:
    """Optimized configuration for production localhost Redis"""
    # Redis connection (localhost)
    # redis_host: str = "localhost"  # or "127.0.0.1"
    redis_host: str = "192.168.29.42"  # or "127.0.0.1"
    redis_port: int = 6379
    redis_password: str = "SuperDuperRedis6748@"

    # Test parameters - OPTIMIZED for localhost
    duration_seconds: int = 30
    target_messages_per_second: int = 2000  # Double the network target!
    batch_size: int = 100  # Larger batches for efficiency

    # Publisher settings - MORE CONCURRENCY
    max_concurrent_tasks: int = 20  # Double for localhost

    # Health monitoring
    health_check_interval: float = 5.0
    circuit_breaker_threshold: int = 3
    circuit_breaker_timeout: float = 30.0
    stats_log_interval: float = 10.0

    @property
    def batches_per_second(self) -> int:
        """Calculate how many batches needed per second"""
        return self.target_messages_per_second // self.batch_size

    @property
    def time_per_1000_messages(self) -> float:
        """Expected time to send 1000 messages"""
        return 1000 / self.target_messages_per_second


class StressTestRunner:
    """Handles the execution of the stress test"""

    def __init__(self, config: ProductionStressTestConfig):
        self.config = config
        self.connector = None
        self.publisher = None
        self.total_sent = 0
        self.total_failed = 0
        self.start_time = None
        self.end_time = None

    async def setup(self):
        """Initialize Redis connector and publisher"""
        logger.info("🚀 Setting up PRODUCTION-OPTIMIZED stress test...")
        logger.info(f"   Target: {self.config.target_messages_per_second} msgs/sec")
        logger.info(f"   Expected time for 1000 msgs: {self.config.time_per_1000_messages:.3f}s")

        # Create connector with health monitoring
        self.connector = RedisConnector(
            host=self.config.redis_host,
            port=self.config.redis_port,
            password=self.config.redis_password,
            health_check_interval=self.config.health_check_interval,
            circuit_breaker_threshold=self.config.circuit_breaker_threshold,
            circuit_breaker_timeout=self.config.circuit_breaker_timeout,
            stats_log_interval=self.config.stats_log_interval
        )

        # Create publisher
        self.publisher = HighSpeedPublisher(
            self.connector,
            max_concurrent_tasks=self.config.max_concurrent_tasks
        )

        logger.info(f"✅ Setup complete")
        logger.info(f"   Redis: {self.config.redis_host}:{self.config.redis_port}")
        logger.info(f"   Batch size: {self.config.batch_size} msgs")
        logger.info(f"   Batches/sec: {self.config.batches_per_second}")
        logger.info(f"   Max concurrent: {self.config.max_concurrent_tasks} tasks")

    async def generate_mock_prices(self, iteration: int) -> dict:
        """Generate mock price data for testing"""
        return {
            f"PAIR_{i}": 1000.0 + iteration + i * 0.1
            for i in range(self.config.batch_size)
        }

    async def run_test(self):
        """Execute the stress test"""
        logger.info(f"\n{'='*60}")
        logger.info("🏁 STARTING PRODUCTION STRESS TEST")
        logger.info(f"{'='*60}\n")

        self.start_time = time.perf_counter()
        iteration = 0

        try:
            for second in range(self.config.duration_seconds):
                second_start = time.perf_counter()
                second_sent = 0
                second_failed = 0

                # Send batches as fast as possible
                for batch in range(self.config.batches_per_second):
                    mock_prices = await self.generate_mock_prices(iteration)
                    success = await self.publisher.publish_prices(mock_prices)

                    if success:
                        second_sent += self.config.batch_size
                        self.total_sent += self.config.batch_size
                    else:
                        second_failed += self.config.batch_size
                        self.total_failed += self.config.batch_size

                    iteration += 1

                # Wait for remaining time in this second (if any)
                second_duration = time.perf_counter() - second_start
                remaining_time = 1.0 - second_duration
                if remaining_time > 0:
                    await asyncio.sleep(remaining_time)

                # Print progress
                second_duration = time.perf_counter() - second_start
                health_status = "✅" if self.connector.is_healthy else "❌"
                logger.info(
                    f"Second {second + 1:2d}/{self.config.duration_seconds}: "
                    f"Sent: {second_sent:5d} | "
                    f"Failed: {second_failed:4d} | "
                    f"Duration: {second_duration:.3f}s | "
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
        stats = self.connector.get_stats()
        actual_throughput = self.total_sent / duration

        logger.info(f"\n{'='*60}")
        logger.info("📊 PRODUCTION STRESS TEST RESULTS")
        logger.info(f"{'='*60}")
        logger.info(f"Total Duration:        {duration:.2f} seconds")
        logger.info(f"Messages Sent:         {stats['messages_sent']:,}")
        logger.info(f"Messages Dropped:      {stats['messages_dropped']:,}")
        logger.info(f"Messages Congested:    {stats['messages_congested']:,}")
        logger.info(f"Success Rate:          {stats['success_rate']}")
        logger.info(f"Actual Throughput:     {actual_throughput:.2f} msgs/sec")
        logger.info(f"Target Throughput:     {self.config.target_messages_per_second} msgs/sec")

        # Performance assessment
        efficiency = actual_throughput / self.config.target_messages_per_second * 100
        logger.info(f"Efficiency:            {efficiency:.1f}%")

        # Time for 1000 messages
        time_per_1000 = 1000 / actual_throughput
        logger.info(f"\n⏱️ Time for 1000 messages: {time_per_1000:.3f} seconds")

        if time_per_1000 <= 0.5:
            logger.info(f"🎉 Target ACHIEVED! (≤0.5s)")
        elif time_per_1000 <= 0.55:
            logger.info(f"✅ Very close to target!")
        elif time_per_1000 <= 0.7:
            logger.info(f"⚠️ Good performance, but above 0.5s target")
        else:
            logger.info(f"❌ Below target - may need further optimization")

        if efficiency >= 95:
            logger.info(f"Assessment:            🎉 Excellent - Target achieved!")
        elif efficiency >= 80:
            logger.info(f"Assessment:            ✅ Good - Close to target")
        elif efficiency >= 50:
            logger.info(f"Assessment:            ⚠️ Fair - Below target")
        else:
            logger.info(f"Assessment:            ❌ Poor - Significantly below target")

        logger.info(f"{'='*60}\n")

    async def cleanup(self):
        """Clean up resources"""
        logger.info("Cleaning up...")
        if self.connector:
            await self.connector.close()
        logger.info("✅ Cleanup complete")


async def main():
    """Main entry point for production stress test"""
    # Create OPTIMIZED configuration for localhost
    config = ProductionStressTestConfig(
        redis_host="localhost",          # Same machine as Redis
        duration_seconds=30,
        target_messages_per_second=2000, # 2× network target
        batch_size=100,                  # 2× network batch size
        max_concurrent_tasks=20          # 2× network concurrency
    )

    # Run stress test
    runner = StressTestRunner(config)

    try:
        await runner.setup()
        await runner.run_test()
        await runner.print_results()
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    print("\n" + "="*60)
    print("🎯 PRODUCTION STRESS TEST - LOCALHOST OPTIMIZED")
    print("="*60)
    print("Configuration:")
    print("  - Redis: localhost (same machine)")
    print("  - Target: 2000 msgs/sec (0.5s per 1000 msgs)")
    print("  - Batch size: 100 messages")
    print("  - Concurrency: 20 tasks")
    print("="*60 + "\n")

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⚠️ Test terminated by user")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)

