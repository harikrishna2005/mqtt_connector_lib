# Smart Executor Demo

This project demonstrates an advanced smart auto-scaling task executor with:

- Dynamic worker scaling (up/down)
- CPU EWMA smoothing
- Memory usage monitoring
- Queue utilization % tracking
- CSV metrics logging
- Live Rich terminal dashboard
- Load generator for stress testing
- Graph plotting utilities

---

## Project Structure

smart_executor_demo/
├── README.md
├── smart_executor.py
├── metrics_collector.py
├── load_generator.py
├── dashboard.py
├── plot_metrics.py
└── run_demo.py

---

## How to Run

### 1. Create virtual environment

python -m venv venv
source venv/bin/activate        # Linux/macOS
venv\Scripts\activate           # Windows

### 2. Install dependencies

pip install -r requirements.txt

### 3. Run the demo

python run_demo.py

This will:
- Start the SmartScalingExecutor
- Start the LoadGenerator
- Start MetricsCollector
- Show live dashboard with CPU/MEM/Queue/Workers
- Continuously log metrics to metrics.csv

---

## Plot Metrics After Running

python plot_metrics.py

This generates:

cpu_usage.png
memory_usage.png
queue_usage.png
worker_count.png

---

## Notes

This example uses asyncio and is suitable for building scalable backend systems, including MQTT consumers, websocket processors, and message-stream processors. You can integrate SmartScalingExecutor into any event-driven architecture easily.

