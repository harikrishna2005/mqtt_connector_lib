"""
Prometheus metrics exporter for SmartScalingExecutor.

This module exposes metrics that can be scraped by Prometheus and visualized in Grafana.

Usage:
    1. Start the metrics HTTP server: await start_prometheus_server(port=8000)
    2. Use the callback: metrics_cb=prometheus_metrics_callback
    3. Configure Prometheus to scrape: http://localhost:8000/metrics
    4. Create Grafana dashboard with Prometheus as data source
"""

from prometheus_client import Gauge, start_http_server, REGISTRY
import asyncio
import logging

logger = logging.getLogger(__name__)


# Define Prometheus metrics
# Gauge = metric that can go up or down (perfect for queue size, workers, CPU, etc.)

executor_queue_size = Gauge(
    'smart_executor_queue_size',
    'Current number of messages in the executor queue'
)

executor_queue_usage = Gauge(
    'smart_executor_queue_usage_percent',
    'Queue usage as percentage (0-100)'
)

executor_worker_count = Gauge(
    'smart_executor_worker_count',
    'Number of active worker tasks'
)

executor_cpu_ewma = Gauge(
    'smart_executor_cpu_ewma_percent',
    'CPU usage exponentially weighted moving average (0-100)'
)

executor_memory_usage = Gauge(
    'smart_executor_memory_usage_percent',
    'Memory usage percentage (0-100)'
)


def prometheus_metrics_callback(cpu, mem, qsize, qusage, workers):
    """
    Callback function to update Prometheus metrics.

    This can be passed directly to SmartScalingExecutor's metrics_cb parameter.

    Args:
        cpu: CPU EWMA percentage (0-100)
        mem: Memory usage percentage (0-100)
        qsize: Current queue size (absolute number)
        qusage: Queue usage as ratio (0.0-1.0)
        workers: Number of active workers
    """
    try:
        executor_cpu_ewma.set(cpu)
        executor_memory_usage.set(mem)
        executor_queue_size.set(qsize)
        executor_queue_usage.set(qusage * 100)  # Convert to percentage
        executor_worker_count.set(workers)
    except Exception as e:
        logger.error(f"Failed to update Prometheus metrics: {e}")


async def start_prometheus_server(port=8000):
    """
    Start Prometheus HTTP server in a background thread.

    Prometheus will scrape metrics from http://localhost:{port}/metrics

    Args:
        port: HTTP port to expose metrics (default: 8000)
    """
    try:
        # start_http_server runs in a background thread (non-blocking)
        start_http_server(port)
        logger.info(f"Prometheus metrics server started on port {port}")
        logger.info(f"Metrics available at: http://localhost:{port}/metrics")
        logger.info("Configure Prometheus to scrape this endpoint.")
        return True
    except OSError as e:
        logger.error(f"Failed to start Prometheus server on port {port}: {e}")
        logger.error("The port might already be in use.")
        return False
    except Exception as e:
        logger.error(f"Unexpected error starting Prometheus server: {e}")
        return False


# Optional: Combined callback that updates both CSV and Prometheus
def combined_metrics_callback(metrics_collector, csv_filepath="metrics.csv"):
    """
    Returns a callback that updates both CSV (via MetricsCollector) and Prometheus.

    Usage:
        from metrics_collector import MetricsCollector
        from prometheus_metrics import combined_metrics_callback

        collector = MetricsCollector("metrics.csv")
        callback = combined_metrics_callback(collector)

        executor = SmartScalingExecutor(..., metrics_cb=callback)
    """
    def callback(cpu, mem, qsize, qusage, workers):
        # Update CSV
        metrics_collector.record(cpu, mem, qsize, qusage, workers)
        # Update Prometheus
        prometheus_metrics_callback(cpu, mem, qsize, qusage, workers)

    return callback


if __name__ == "__main__":
    """
    Test the Prometheus metrics server.
    Run: python prometheus_metrics.py
    Then visit: http://localhost:8000/metrics
    """
    import time

    print("Starting Prometheus metrics test server...")
    start_http_server(8000)
    print("Metrics server running on http://localhost:8000/metrics")
    print("Press Ctrl+C to stop")

    # Simulate updating metrics
    try:
        counter = 0
        while True:
            counter += 1
            # Simulate some metrics
            prometheus_metrics_callback(
                cpu=50 + (counter % 30),
                mem=60 + (counter % 20),
                qsize=100 + (counter % 50),
                qusage=0.5 + (counter % 50) / 100,
                workers=5 + (counter % 10)
            )
            print(f"Updated metrics (iteration {counter})")
            time.sleep(2)
    except KeyboardInterrupt:
        print("\nStopping test server...")

