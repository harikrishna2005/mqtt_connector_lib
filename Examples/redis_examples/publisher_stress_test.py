"""
Redis High-Speed Publisher Stress Test

Tests the publisher's ability to handle high-frequency message publishing
with automatic health monitoring and circuit breaker protection.

Features:
- Configurable message rate (msgs/sec)
- Automatic health monitoring
- Circuit breaker for Redis failures
- Real-time statistics
- Performance metrics
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
class StressTestConfig:
    """Configuration for stress test"""
    # Redis connection
    redis_host: str = "192.168.29.42"
    redis_port: int = 6379
    redis_password: str = "SuperDuperRedis6748@"

    # Test parameters
    duration_seconds: int = 30
    target_messages_per_second: int = 1000
    batch_size: int = 50  # Number of price pairs per batch

    # Publisher settings
    max_concurrent_tasks: int = 10

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
    def sleep_between_batches(self) -> float:
        """Calculate sleep time between batches"""
        return 1.0 / self.batches_per_second


class StressTestRunner:
    """Handles the execution of the stress test"""

    def __init__(self, config: StressTestConfig):
        self.config = config
        self.connector = None
        self.publisher = None
        self.total_sent = 0
        self.total_failed = 0
        self.start_time = None
        self.end_time = None

    async def setup(self):
        """Initialize Redis connector and publisher"""
        logger.info("Setting up stress test environment...")

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
        logger.info(f"   Health monitoring: Auto-started")

    async def generate_mock_prices(self, iteration: int) -> dict:
        """Generate mock price data for testing"""
        return {
            f"PAIR_{i}": 1000.0 + iteration + i * 0.1
            for i in range(self.config.batch_size)
        }

    async def run_test(self):
        """Execute the stress test"""
        logger.info(f"\n🚀 Starting Stress Test")
        logger.info(f"{'='*60}")
        logger.info(f"Target: {self.config.target_messages_per_second} msgs/sec")
        logger.info(f"Duration: {self.config.duration_seconds} seconds")
        logger.info(f"Batch size: {self.config.batch_size} messages")
        logger.info(f"Batches per second: {self.config.batches_per_second}")
        logger.info(f"{'='*60}\n")

        self.start_time = time.perf_counter()
        iteration = 0

        try:
            for second in range(self.config.duration_seconds):
                second_start = time.perf_counter()
                second_sent = 0
                second_failed = 0

                # Send batches for this second AS FAST AS POSSIBLE
                # No sleep - measure true maximum throughput
                for batch in range(self.config.batches_per_second):
                    # Generate and publish prices
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
                    f"Sent: {second_sent:4d} | "
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

        logger.info(f"\n{'='*60}")
        logger.info("📊 STRESS TEST RESULTS")
        logger.info(f"{'='*60}")
        logger.info(f"Total Duration:        {duration:.2f} seconds")
        logger.info(f"Messages Sent:         {stats['messages_sent']:,}")
        logger.info(f"Messages Dropped:      {stats['messages_dropped']:,}")
        logger.info(f"Messages Congested:    {stats['messages_congested']:,}")
        logger.info(f"Success Rate:          {stats['success_rate']}")
        logger.info(f"Actual Throughput:     {self.total_sent / duration:.2f} msgs/sec")
        logger.info(f"Target Throughput:     {self.config.target_messages_per_second} msgs/sec")

        # Performance assessment
        efficiency = (self.total_sent / duration) / self.config.target_messages_per_second * 100
        logger.info(f"Efficiency:            {efficiency:.1f}%")

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
    """Main entry point for stress test"""
    # Create configuration
    config = StressTestConfig(
        duration_seconds=30,
        target_messages_per_second=1000,
        batch_size=50,  # Optimal for current network conditions
        max_concurrent_tasks=20
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
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⚠️ Test terminated by user")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)

