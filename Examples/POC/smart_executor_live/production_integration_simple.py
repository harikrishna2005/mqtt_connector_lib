"""
Production Integration Example - MQTT Library with SmartScalingExecutor
No separate process needed - metrics tracked within your application
"""

import asyncio
import logging
from smart_scaling_executor import SmartScalingExecutor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ProductionMetricsCollector:
    """Simple metrics collector that logs to console/file"""

    def __init__(self):
        self.metrics_history = []

    def collect(self, **metrics):
        """Called every 2 seconds by SmartScalingExecutor"""
        # Log important metrics
        logger.info(
            f"METRICS | Workers: {metrics['workers']} | "
            f"Queue: {metrics['qsize']} | "
            f"CPU: {metrics['cpu']:.1f}% | "
            f"QueueUsage: {metrics['qusage']*100:.1f}%"
        )

        # Store for analysis (optional)
        self.metrics_history.append(metrics)

        # Alert on issues (optional)
        if metrics['qusage'] > 0.8:  # 80% queue full
            logger.warning(f"⚠️ HIGH QUEUE USAGE: {metrics['qusage']*100:.1f}%")

        if metrics['workers'] >= 14:  # Near max
            logger.warning(f"⚠️ HIGH WORKER COUNT: {metrics['workers']}")


async def your_mqtt_message_handler(topic: str, payload: bytes):
    """Your actual MQTT message handler"""
    # Process the message
    logger.debug(f"Processing: {topic} - {payload}")
    await asyncio.sleep(0.01)  # Simulate work


class YourMQTTApplication:
    """Your MQTT application with integrated SmartScalingExecutor"""

    def __init__(self):
        # Create metrics collector
        self.metrics_collector = ProductionMetricsCollector()

        # Create executor with metrics callback
        self.executor = SmartScalingExecutor(
            min_workers=5,
            max_workers=15,
            queue_size=2000,
            metrics_cb=self.metrics_collector.collect  # ← Metrics tracked here!
        )

    async def start(self):
        """Start your application"""
        logger.info("🚀 Starting MQTT application...")

        # Start the executor
        await self.executor.start()
        logger.info(f"✅ SmartScalingExecutor started with {len(self.executor.workers)} workers")

        # Your MQTT connection setup here
        # mqtt_client = await connect_to_broker(...)
        # mqtt_client.on_message = self.on_mqtt_message

    async def on_mqtt_message(self, topic: str, payload: bytes, qos: int):
        """Called when MQTT message arrives"""
        # Submit to executor - NO NEED TO AWAIT
        success = self.executor.submit(
            topic=topic,
            payload=payload,
            handler=your_mqtt_message_handler
        )

        if not success:
            logger.error(f"❌ Failed to submit message - queue full!")

    async def stop(self):
        """Stop your application"""
        logger.info("⏹️  Stopping MQTT application...")
        await self.executor.stop()
        logger.info("✅ Stopped")


# =============================================================================
# PRODUCTION USAGE
# =============================================================================

async def main():
    """Your production entry point"""
    app = YourMQTTApplication()

    try:
        await app.start()

        # Your application runs here...
        # The executor will log metrics every 2 seconds automatically
        logger.info("Application running... (Ctrl+C to stop)")

        # Keep running
        while True:
            await asyncio.sleep(1)

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        await app.stop()


if __name__ == "__main__":
    asyncio.run(main())

