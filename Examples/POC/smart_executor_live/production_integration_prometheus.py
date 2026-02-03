"""
Production Integration Example - MQTT Library with Prometheus Metrics
Includes Prometheus metrics server for Grafana monitoring
"""

import asyncio
import logging
from smart_scaling_executor import SmartScalingExecutor
from prometheus_metrics import start_prometheus_server, prometheus_metrics_callback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ProductionMetricsHandler:
    """Handles both logging and Prometheus metrics"""

    def __init__(self):
        self.metrics_count = 0

    def handle_metrics(self, **metrics):
        """Called every 2 seconds by SmartScalingExecutor"""
        self.metrics_count += 1

        # Send to Prometheus
        prometheus_metrics_callback(**metrics)

        # Log every 30 seconds (not every 2 seconds to avoid spam)
        if self.metrics_count % 15 == 0:
            logger.info(
                f"METRICS | Workers: {metrics['workers']:2d} | "
                f"Queue: {metrics['qsize']:4d} | "
                f"CPU: {metrics['cpu']:5.1f}% | "
                f"Mem: {metrics['mem']:5.1f}% | "
                f"QUsage: {metrics['qusage']*100:5.1f}%"
            )

        # Alerts
        if metrics['qusage'] > 0.8:
            logger.warning(f"⚠️ HIGH QUEUE: {metrics['qusage']*100:.1f}% full")

        if metrics['workers'] >= 14:
            logger.warning(f"⚠️ NEAR MAX WORKERS: {metrics['workers']}/15")


async def your_mqtt_message_handler(topic: str, payload: bytes):
    """Your actual MQTT message handler"""
    logger.debug(f"Processing: {topic}")
    # Your message processing logic here
    await asyncio.sleep(0.01)  # Simulate work


class YourMQTTApplicationWithPrometheus:
    """Your MQTT application with Prometheus metrics"""

    def __init__(self, prometheus_port=8000):
        self.prometheus_port = prometheus_port
        self.metrics_handler = ProductionMetricsHandler()

        # Create executor with metrics callback
        self.executor = SmartScalingExecutor(
            min_workers=5,
            max_workers=15,
            queue_size=2000,
            metrics_cb=self.metrics_handler.handle_metrics
        )

    async def start(self):
        """Start your application"""
        logger.info("🚀 Starting MQTT application with Prometheus metrics...")

        # Start Prometheus metrics server
        success = await start_prometheus_server(port=self.prometheus_port)
        if success:
            logger.info(f"✅ Prometheus metrics server started on port {self.prometheus_port}")
            logger.info(f"📊 Metrics available at: http://localhost:{self.prometheus_port}/metrics")
        else:
            logger.error(f"❌ Failed to start Prometheus server on port {self.prometheus_port}")
            logger.info("⚠️  Continuing without Prometheus metrics...")

        # Start the executor
        await self.executor.start()
        logger.info(f"✅ SmartScalingExecutor started with {len(self.executor.workers)} workers")

        # Your MQTT connection setup here
        logger.info("📡 Connect to MQTT broker here...")
        # mqtt_client = await connect_to_broker(...)
        # mqtt_client.on_message = self.on_mqtt_message

    async def on_mqtt_message(self, topic: str, payload: bytes, qos: int):
        """Called when MQTT message arrives"""
        # Submit to executor
        success = self.executor.submit(
            topic=topic,
            payload=payload,
            handler=your_mqtt_message_handler
        )

        if not success:
            logger.error(f"❌ Queue full! Message dropped: {topic}")

    async def stop(self):
        """Stop your application"""
        logger.info("⏹️  Stopping MQTT application...")
        await self.executor.stop()
        logger.info("✅ Application stopped")


# =============================================================================
# PRODUCTION USAGE
# =============================================================================

async def main():
    """Your production entry point"""

    # Create app with Prometheus on port 8000
    app = YourMQTTApplicationWithPrometheus(prometheus_port=8000)

    try:
        await app.start()

        logger.info("="*70)
        logger.info("🎯 Application is running!")
        logger.info("="*70)
        logger.info("📊 Metrics available at: http://localhost:8000/metrics")
        logger.info("💡 Setup Prometheus to scrape this endpoint")
        logger.info("💡 Create Grafana dashboard for visualization")
        logger.info("⏹️  Press Ctrl+C to stop")
        logger.info("="*70)

        # Keep running
        while True:
            await asyncio.sleep(60)  # Check every minute

    except KeyboardInterrupt:
        logger.info("\n🛑 Interrupted by user")
    finally:
        await app.stop()


if __name__ == "__main__":
    asyncio.run(main())

