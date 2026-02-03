# 📊 Viewing Metrics Without Grafana - Quick Guide

## 🎯 4 Simple Ways to View Metrics

### Option 1: Terminal Dashboard (Best for Real-Time Monitoring) ⭐

**What:** Live ASCII dashboard with graphs in your terminal

**How to run:**
```bash
poetry run python view_metrics_simple.py
```

**What you see:**
```
================================================================================
📊 SmartScalingExecutor - Live Metrics Dashboard
================================================================================

⚙️  Workers:     7   ███████             
📦 Queue Size:  350  ███████
📊 Queue Usage: 17.5%
🖥️  CPU:        12.5%  ██
💾 Memory:     68.2%  █████████████

📈 Worker Trend (last 30 intervals):
 20 |          ███      
 16 |        █████      
 12 |      ███████      
  8 |    █████████      
  4 |  ███████████      
  0 | █████████████     

📈 Queue Trend (last 30 intervals):
500 |          █        
400 |        ███        
300 |      █████        
200 |    ███████        
100 |  █████████        
  0 | ███████████       
================================================================================
⏱️  Last updated: 14:35:22
Press Ctrl+C to stop
================================================================================
```

**Pros:**
- ✅ Visual and intuitive
- ✅ Shows trends over time
- ✅ Auto-saves to CSV
- ✅ No external dependencies

**Cons:**
- ⚠️ Terminal only (no web UI)

---

### Option 2: Simple Logging (Easiest) ⭐

**What:** Just print metrics to console

**How to run:**
```bash
poetry run python view_metrics_logging.py
```

**What you see:**

**Compact style:**
```
[14:35:20] Workers:  5 [█████░░░░░░░░░░] | Queue:  150 (7.5%) [█░░░░░░░░░░░░░░░░░░░] | CPU:  10.2% | Mem: 68.5%
[14:35:22] Workers:  7 [███████░░░░░░░░] | Queue:  350 (17.5%) [███░░░░░░░░░░░░░░░░░] | CPU:  15.8% | Mem: 68.7%
[14:35:24] Workers:  6 [██████░░░░░░░░░] | Queue:  120 (6.0%) [█░░░░░░░░░░░░░░░░░░░] | CPU:  12.1% | Mem: 68.6%
```

**Detailed style:**
```
======================================================================
⏰ Timestamp: 14:35:20
⚙️  Workers:    5 / 15 max
📦 Queue:     150 items ( 7.5% usage) 🟢 LOW
🖥️  CPU:       10.2%
💾 Memory:    68.5%
======================================================================

======================================================================
⏰ Timestamp: 14:35:22
⚙️  Workers:    7 / 15 max
📦 Queue:     350 items (17.5% usage) 🟢 LOW
🖥️  CPU:       15.8%
💾 Memory:    68.7%
======================================================================
```

**Pros:**
- ✅ Super simple
- ✅ No setup needed
- ✅ Easy to customize

**Cons:**
- ⚠️ No history/trends
- ⚠️ Manual scrolling

---

### Option 3: Raw Prometheus Metrics in Browser

**What:** View raw metrics in browser (no dashboard)

**How to use:**

```python
from prometheus_metrics import start_prometheus_server, prometheus_metrics_callback

await start_prometheus_server(port=8000)
executor = SmartScalingExecutor(metrics_cb=prometheus_metrics_callback)
```

**Then open:** http://localhost:8000/metrics

**What you see:**
```
# HELP smart_executor_worker_count Current number of active workers
# TYPE smart_executor_worker_count gauge
smart_executor_worker_count 7

# HELP smart_executor_queue_size Current queue size
# TYPE smart_executor_queue_size gauge
smart_executor_queue_size 350

# HELP smart_executor_cpu_ewma_percent CPU EWMA percentage
# TYPE smart_executor_cpu_ewma_percent gauge
smart_executor_cpu_ewma_percent 15.8

# HELP smart_executor_memory_usage_percent Memory usage percentage
# TYPE smart_executor_memory_usage_percent gauge
smart_executor_memory_usage_percent 68.7

# HELP smart_executor_queue_usage_percent Queue usage percentage
# TYPE smart_executor_queue_usage_percent gauge
smart_executor_queue_usage_percent 17.5
```

**Pros:**
- ✅ No dependencies
- ✅ Browser-based
- ✅ Good for spot checks

**Cons:**
- ⚠️ Not visual
- ⚠️ Manual refresh needed
- ⚠️ Plain text format

---

### Option 4: CSV Export (Best for Analysis)

**What:** Save metrics to CSV, analyze later

**How to use:**

```python
from metrics_collector import MetricsCollector

collector = MetricsCollector(filename="metrics.csv")
executor = SmartScalingExecutor(metrics_cb=collector.collect)

# Metrics auto-saved to metrics.csv
```

**View with:**

**Excel:** Just open `metrics.csv`

**Python pandas:**
```python
import pandas as pd
df = pd.read_csv("metrics.csv")
print(df.describe())
```

**Plot graphs:**
```bash
poetry run python plot_metrics.py
```

**What you get:**
```csv
timestamp,cpu,mem,qsize,qusage,workers
1732800920,10.2,68.5,150,0.075,5
1732800922,15.8,68.7,350,0.175,7
1732800924,12.1,68.6,120,0.06,6
```

**Pros:**
- ✅ Historical data
- ✅ Easy to graph
- ✅ Works with Excel/Python
- ✅ Good for reports

**Cons:**
- ⚠️ Not real-time
- ⚠️ Need separate tool to view

---

## 🎓 Which Option to Choose?

### For Development/Testing:
**→ Use Option 1: Terminal Dashboard** ⭐
```bash
poetry run python view_metrics_simple.py
```
Best balance of visual feedback and simplicity.

### For Quick Checks:
**→ Use Option 2: Simple Logging** ⭐
```bash
poetry run python view_metrics_logging.py
```
Quickest to set up, good enough for basic monitoring.

### For Production:
**→ Use Prometheus + Grafana** (or Option 4: CSV Export)
- Full monitoring solution
- Alerts and notifications
- Historical analysis

### For Analysis:
**→ Use Option 4: CSV Export**
```python
collector = MetricsCollector(filename="metrics.csv")
executor = SmartScalingExecutor(metrics_cb=collector.collect)
```
Best for detailed analysis, graphing, reports.

---

## 💡 Pro Tips

### Combine Multiple Options:

```python
from metrics_collector import MetricsCollector

collector = MetricsCollector(filename="metrics.csv")

def combined_callback(**metrics):
    # Log to console
    print(f"Workers: {metrics['workers']}, Queue: {metrics['qsize']}")
    # Save to CSV
    collector.collect(**metrics)

executor = SmartScalingExecutor(metrics_cb=combined_callback)
```

### Custom Alerts:

```python
def alert_callback(**metrics):
    if metrics['qsize'] > 1000:
        print("🚨 ALERT: Queue is too high!")
    if metrics['workers'] >= 14:
        print("⚠️ WARNING: Near max workers!")

executor = SmartScalingExecutor(metrics_cb=alert_callback)
```

---

## 🚀 Quick Start Commands

```bash
# Option 1: Terminal dashboard (recommended)
poetry run python view_metrics_simple.py

# Option 2: Simple logging
poetry run python view_metrics_logging.py

# Option 3: Prometheus metrics (then open http://localhost:8000/metrics)
# (Use prometheus_metrics_callback in your code)

# Option 4: CSV export (auto-saves to metrics.csv)
# (Use MetricsCollector in your code)
```

---

## ❓ FAQ

**Q: Do I need Grafana?**  
A: No! Use Option 1 or 2 for simple monitoring.

**Q: Can I save metrics AND see them live?**  
A: Yes! Use Option 1 (view_metrics_simple.py) - it does both!

**Q: What's the simplest option?**  
A: Option 2 (simple logging) - just prints to console.

**Q: What's the best for testing?**  
A: Option 1 (terminal dashboard) - visual + saves CSV.

**Q: Can I use in production without Grafana?**  
A: Yes, but for production, Grafana is recommended for alerts, history, and better visualization.

---

**You don't need Grafana! Pick the option that works for you!** 📊

