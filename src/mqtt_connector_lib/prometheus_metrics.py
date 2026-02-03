"""
Prometheus metrics support for SmartScalingExecutor.

This module provides Prometheus metrics integration with optional terminal dashboard.
"""

import asyncio
import logging
from prometheus_client import Gauge, start_http_server

logger = logging.getLogger(__name__)

# Prometheus metrics
smart_executor_worker_count = Gauge(
    'smart_executor_worker_count',
    'Current number of active workers'
)

smart_executor_queue_size = Gauge(
    'smart_executor_queue_size',
    'Current queue size'
)

smart_executor_cpu_ewma_percent = Gauge(
    'smart_executor_cpu_ewma_percent',
    'CPU EWMA percentage'
)

smart_executor_memory_usage_percent = Gauge(
    'smart_executor_memory_usage_percent',
    'Memory usage percentage'
)

smart_executor_queue_usage_percent = Gauge(
    'smart_executor_queue_usage_percent',
    'Queue usage percentage (0-100)'
)


def prometheus_metrics_callback(**metrics):
    """
    Callback function to update Prometheus metrics.

    Args:
        metrics: Dictionary containing:
            - workers: int
            - qsize: int
            - cpu: float
            - mem: float
            - qusage: float (0.0-1.0)
    """
    smart_executor_worker_count.set(metrics.get('workers', 0))
    smart_executor_queue_size.set(metrics.get('qsize', 0))
    smart_executor_cpu_ewma_percent.set(metrics.get('cpu', 0))
    smart_executor_memory_usage_percent.set(metrics.get('mem', 0))
    smart_executor_queue_usage_percent.set(metrics.get('qusage', 0) * 100)


async def start_prometheus_server(port: int = 8000):
    """
    Start Prometheus HTTP server.

    Args:
        port: Port to expose metrics on (default: 8000)
    """
    try:
        start_http_server(port)
        logger.info(f"✅ Prometheus metrics server started on port {port}")
        logger.info(f"📊 Metrics available at: http://localhost:{port}/metrics")
    except OSError as e:
        if "address already in use" in str(e).lower():
            logger.warning(f"⚠️  Port {port} already in use. Metrics server may already be running.")
        else:
            logger.error(f"❌ Failed to start Prometheus server: {e}")
            raise


# Optional: Terminal dashboard that works alongside Prometheus
class TerminalDashboard:
    """
    Optional terminal dashboard that can be used alongside Prometheus.
    Displays metrics in terminal while also exporting to Prometheus.
    """

    def __init__(self, update_interval: float = 2.0):
        self.metrics_history = []
        self.max_history = 30
        self.update_interval = update_interval
        self.last_update_time = 0
        self.latest_metrics = {}
        self._updating = False  # Simple flag to prevent concurrent updates

    def display(self, **metrics):
        """Display metrics in terminal (throttled to avoid flickering)"""
        import time
        current_time = time.time()

        # Always store the latest metrics
        self.latest_metrics = metrics

        # Only update display if enough time has passed AND not currently updating
        if (not self._updating and
            current_time - self.last_update_time >= self.update_interval):
            self._updating = True
            try:
                self._render_dashboard()
                self.last_update_time = current_time
            finally:
                self._updating = False

    def _render_dashboard(self):
        """Internal method to render the dashboard"""
        import time
        import os

        # Store in history
        self.metrics_history.append(self.latest_metrics.copy())
        if len(self.metrics_history) > self.max_history:
            self.metrics_history.pop(0)

        # Build the entire dashboard as a single string
        dashboard_lines = []

        # Header
        dashboard_lines.append("=" * 80)
        dashboard_lines.append("📊 SmartScalingExecutor - Live Metrics Dashboard")
        dashboard_lines.append("=" * 80)
        dashboard_lines.append("")

        # Current metrics
        workers = self.latest_metrics.get('workers', 0)
        qsize = self.latest_metrics.get('qsize', 0)
        qusage = self.latest_metrics.get('qusage', 0)
        cpu = self.latest_metrics.get('cpu', 0)
        mem = self.latest_metrics.get('mem', 0)

        dashboard_lines.append(f"⚙️  Workers:     {workers:3d}   {'█' * min(workers, 20)}{' ' * max(0, 20 - workers)}")
        dashboard_lines.append(f"📦 Queue Size:  {qsize:4d}  {'█' * min(qsize // 50, 40)}")
        dashboard_lines.append(f"📊 Queue Usage: {qusage*100:5.1f}%")
        dashboard_lines.append(f"🖥️  CPU:        {cpu:5.1f}%  {'█' * int(min(cpu, 100) / 5)}")
        dashboard_lines.append(f"💾 Memory:     {mem:5.1f}%  {'█' * int(min(mem, 100) / 5)}")
        dashboard_lines.append("")

        # Trend (simplified to avoid complex sparklines)
        if len(self.metrics_history) >= 5:
            recent_workers = [m.get('workers', 0) for m in self.metrics_history[-5:]]
            recent_queues = [m.get('qsize', 0) for m in self.metrics_history[-5:]]

            dashboard_lines.append("📈 Recent Trends (last 5 updates):")
            dashboard_lines.append(f"   Workers: {' → '.join(map(str, recent_workers))}")
            dashboard_lines.append(f"   Queue:   {' → '.join(map(str, recent_queues))}")

        dashboard_lines.append("")
        dashboard_lines.append("=" * 80)
        dashboard_lines.append(f"⏱️  Last updated: {time.strftime('%H:%M:%S')} (Updates every {self.update_interval}s)")
        dashboard_lines.append("📊 Prometheus metrics: http://localhost:8000/metrics")
        dashboard_lines.append("=" * 80)

        # Clear screen completely and print entire dashboard at once
        if os.name == 'nt':  # Windows
            os.system('cls')
        else:  # Unix/Linux/Mac
            os.system('clear')

        # Print entire dashboard as one operation
        print('\n'.join(dashboard_lines), flush=True)

    def _print_sparkline(self, data, max_value):
        """Print ASCII sparkline chart"""
        if not data:
            return

        normalized = [min(10, int(val / max_value * 10)) for val in data]

        for level in range(10, -1, -1):
            line = ""
            for val in normalized:
                if val >= level:
                    line += "█"
                else:
                    line += " "
            if level % 2 == 0:
                print(f"{int(level * max_value / 10):4d} | {line}")
            else:
                print(f"     | {line}")


def create_combined_callback(enable_terminal_dashboard: bool = True, terminal_update_interval: float = 2.0):
    """
    Create a combined callback that updates both Prometheus AND terminal dashboard.

    Args:
        enable_terminal_dashboard: If True, also displays metrics in terminal
        terminal_update_interval: How often to update terminal display (seconds)

    Returns:
        Callback function that can be passed to SmartScalingExecutor

    Usage:
        from mqtt_connector_lib.prometheus_metrics import start_prometheus_server, create_combined_callback
        from mqtt_connector_lib.gmqtt_connector import set_metrics_callback

        # Start Prometheus server
        await start_prometheus_server(port=8000)

        # Create combined callback (Prometheus + Terminal, updates every 2 seconds)
        callback = create_combined_callback(enable_terminal_dashboard=True, terminal_update_interval=2.0)
        set_metrics_callback(callback)

        # Now create your GMqttConnector - metrics will be exported to both!
    """
    dashboard = TerminalDashboard(update_interval=terminal_update_interval) if enable_terminal_dashboard else None

    def combined_callback(**metrics):
        # Always update Prometheus (real-time)
        prometheus_metrics_callback(**metrics)

        # Optionally display in terminal (throttled)
        if dashboard:
            dashboard.display(**metrics)

    return combined_callback

