# 🚀 Quick Start Guide - SmartScalingExecutor

## How to Start and Monitor

### Option 1: Quick Demo (30 seconds)
```bash
cd Examples/POC/smart_executor_live
poetry run python test_prometheus_demo.py
```
**What it does:**
- Runs for 30 seconds (configurable)
- Generates bursts of 300-1000 messages
- Shows real-time metrics in terminal
- Exposes Prometheus metrics on http://localhost:8000/metrics

---

### Option 2: Full Demo with Dashboard (Run until you stop)
```bash
cd Examples/POC/smart_executor_live
poetry run python run_demo_with_prometheus.py
```
**What it does:**
- Runs indefinitely (Ctrl+C to stop)
- Generates continuous load
- Terminal dashboard with live metrics
- Prometheus metrics on http://localhost:8000/metrics
- Saves to `metrics.csv`

---

### Option 3: Your Own Application

Create your script:

```python
import asyncio
from smart_scaling_executor import SmartScalingExecutor

async def my_handler(topic, payload):
    """Your message handler"""
    print(f"Processing: {topic} - {payload}")
    await asyncio.sleep(0.1)  # Simulate work

async def metrics_callback(**metrics):
    """Track metrics (optional)"""
    print(f"Queue: {metrics['qsize']}, Workers: {metrics['workers']}, CPU: {metrics['cpu']:.1f}%")

async def main():
    # Create executor
    executor = SmartScalingExecutor(
        min_workers=5,
        max_workers=15,
        metrics_cb=metrics_callback  # Optional
    )
    
    # Start it
    await executor.start()
    
    # Submit work
    for i in range(1000):
        executor.submit(
            topic="my/topic",
            payload=f"message_{i}",
            handler=my_handler
        )
    
    # Wait for processing
    await asyncio.sleep(10)
    
    # Stop
    await executor.stop()

if __name__ == "__main__":
    asyncio.run(main())
```

---

## 📊 How to Track Metrics

### Method 1: Terminal Output (Real-time)
When you run the demo, you'll see:
```
Time     Queue    Workers    CPU        Status
----------------------------------------------------------------------
  0s     1066     4                     Processing...
  2s     472      4                     Processing...
  4s     0        5                     Idle
```

### Method 2: Prometheus Endpoint
While running, visit in browser:
```
http://localhost:8000/metrics
```

You'll see:
```
smart_executor_queue_size 450.0
smart_executor_worker_count 6.0
smart_executor_cpu_ewma_percent 3.2
smart_executor_memory_usage_percent 74.0
smart_executor_queue_usage_percent 22.5
```

### Method 3: CSV File
After running, check:
```bash
cat metrics.csv
# Or open in Excel
```

Contains:
```
timestamp,cpu_ewma,memory,queue_size,queue_usage,workers
1764180831,0.94,69.8,200,0.1,3
1764180833,1.79,69.8,200,0.1,4
```

### Method 4: Grafana Dashboard (Production)
1. Install Prometheus
2. Configure to scrape http://localhost:8000/metrics
3. Install Grafana
4. Create dashboard
5. See `PROMETHEUS_GRAFANA_SETUP.md` for details

---

## 🎯 What to Watch For

### Good Behavior:
✅ Workers scale 5-7 for small bursts  
✅ Workers scale 8-12 for large bursts  
✅ Workers return to 5-7 when idle  
✅ Queue rarely exceeds 50%  
✅ No message drops  

### Problems to Watch:
❌ Workers constantly at max (15)  
❌ Queue often >80%  
❌ Messages being dropped  
❌ Workers not scaling down  

---

## 📈 Key Metrics Explained

| Metric | What It Means | Good Range |
|--------|---------------|------------|
| **queue_size** | Messages waiting | 0-500 |
| **workers** | Active workers | 5-12 |
| **cpu_ewma** | Smoothed CPU % | 2-30% |
| **queue_usage** | Queue % full | 0-50% |
| **processing_rate** | Msgs/sec | 100-500 |

---

## 🔧 Customization

### Adjust Load:
Edit `load_generator.py`:
```python
LoadGenerator(
    rate=150,  # Steady msgs/sec
    burst_sizes=[300, 500, 800, 1000],  # Random
    burst_interval_range=(3, 5)  # Seconds
)
```

### Adjust Scaling:
Edit `smart_scaling_executor.py`:
```python
SmartScalingExecutor(
    min_workers=5,  # Baseline
    max_workers=15,  # Peak
    queue_check_interval=2.0,  # Check every 2s
    scale_up_cooldown=8.0  # Wait 8s between scale-ups
)
```

---

## 🎯 Quick Commands

```bash
# Run quick demo
poetry run python test_prometheus_demo.py

# Run full demo  
poetry run python run_demo_with_prometheus.py

# View metrics while running
open http://localhost:8000/metrics

# View CSV after running
cat metrics.csv

# Plot metrics (requires matplotlib)
poetry run python plot_metrics.py
```

---

## 🐛 Troubleshooting

**Port 8000 already in use:**
```python
# Edit prometheus_metrics.py, change port
await start_prometheus_server(port=8001)
```

**Too many workers:**
```python
# Decrease max_workers
max_workers=10
```

**Not scaling up:**
```python
# Increase load
rate=300  # More msgs/sec
```

**Not scaling down:**
```python
# Check logs for "SCALE DOWN" events
# Adjust thresholds if needed
```

---

## 📝 Next Steps

1. ✅ Run quick demo to verify it works
2. ✅ Run full demo to see sustained behavior  
3. ✅ Check Prometheus metrics endpoint
4. ✅ Analyze CSV for patterns
5. ✅ Integrate into your application
6. ✅ Setup Grafana for production monitoring

---

**Ready to start? Run this:**
```bash
cd Examples/POC/smart_executor_live
poetry run python run_demo_with_prometheus.py
```

Then open: http://localhost:8000/metrics

**Press Ctrl+C to stop when done!**

