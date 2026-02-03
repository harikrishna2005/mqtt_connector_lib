# 🏭 Production Integration Guide

## ❓ Question: How to Track Metrics in Production?

**Answer: You do NOT need to run any separate Python code!**

The SmartScalingExecutor tracks metrics internally and calls your callback every 2 seconds. Just integrate it into your MQTT application.

---

## 🎯 Two Options

### **Option 1: Simple Logging** (Easiest)
- ✅ No extra setup
- ✅ Metrics logged to console/file
- ✅ Good for development/testing
- ✅ No dependencies

### **Option 2: Prometheus + Grafana** (Production)
- ✅ Professional monitoring
- ✅ Real-time dashboards
- ✅ Alerts and notifications
- ✅ Historical data

---

## 📝 Option 1: Simple Logging Integration

### **Step 1: Import**
```python
from smart_scaling_executor import SmartScalingExecutor
```

### **Step 2: Create Metrics Callback**
```python
def log_metrics(**metrics):
    """Called every 2 seconds automatically"""
    logger.info(
        f"Workers: {metrics['workers']} | "
        f"Queue: {metrics['qsize']} | "
        f"CPU: {metrics['cpu']:.1f}%"
    )
```

### **Step 3: Initialize Executor**
```python
self.executor = SmartScalingExecutor(
    min_workers=5,
    max_workers=15,
    metrics_cb=log_metrics  # ← Your callback
)
```

### **Step 4: Start Executor**
```python
await self.executor.start()
```

### **Step 5: Submit MQTT Messages**
```python
def on_mqtt_message(self, topic, payload, qos):
    """Called when MQTT message arrives"""
    self.executor.submit(
        topic=topic,
        payload=payload,
        handler=your_message_handler
    )
```

### **That's it!** Metrics logged automatically! 🎉

---

## 📊 Option 2: Prometheus Integration

### **Step 1: Import**
```python
from smart_scaling_executor import SmartScalingExecutor
from prometheus_metrics import start_prometheus_server, prometheus_metrics_callback
```

### **Step 2: Start Prometheus Server** (Once in your app)
```python
# In your application startup
await start_prometheus_server(port=8000)
```

### **Step 3: Initialize Executor**
```python
self.executor = SmartScalingExecutor(
    min_workers=5,
    max_workers=15,
    metrics_cb=prometheus_metrics_callback  # ← Prometheus callback
)
```

### **Step 4: Start Executor**
```python
await self.executor.start()
```

### **Step 5: Submit MQTT Messages** (Same as Option 1)
```python
def on_mqtt_message(self, topic, payload, qos):
    self.executor.submit(topic, payload, your_message_handler)
```

### **Step 6: Access Metrics**
```
http://localhost:8000/metrics
```

### **That's it!** Prometheus metrics exposed! 🎉

---

## 📂 Complete Code Examples

### **Example 1: Simple Logging**

File: `production_integration_simple.py`

```python
import asyncio
import logging
from smart_scaling_executor import SmartScalingExecutor

logger = logging.getLogger(__name__)

class YourMQTTApplication:
    def __init__(self):
        # Create executor with logging callback
        self.executor = SmartScalingExecutor(
            min_workers=5,
            max_workers=15,
            metrics_cb=self.log_metrics
        )
    
    def log_metrics(self, **metrics):
        """Automatically called every 2 seconds"""
        logger.info(
            f"Workers: {metrics['workers']} | "
            f"Queue: {metrics['qsize']} | "
            f"CPU: {metrics['cpu']:.1f}%"
        )
    
    async def start(self):
        await self.executor.start()
        # Your MQTT connection here
    
    def on_mqtt_message(self, topic, payload, qos):
        self.executor.submit(topic, payload, your_handler)

# Run your app
async def main():
    app = YourMQTTApplication()
    await app.start()
    # Keep running...
```

### **Example 2: With Prometheus**

File: `production_integration_prometheus.py`

```python
import asyncio
import logging
from smart_scaling_executor import SmartScalingExecutor
from prometheus_metrics import start_prometheus_server, prometheus_metrics_callback

logger = logging.getLogger(__name__)

class YourMQTTApplication:
    def __init__(self):
        self.executor = SmartScalingExecutor(
            min_workers=5,
            max_workers=15,
            metrics_cb=prometheus_metrics_callback  # Prometheus!
        )
    
    async def start(self):
        # Start Prometheus server
        await start_prometheus_server(port=8000)
        logger.info("📊 Metrics: http://localhost:8000/metrics")
        
        # Start executor
        await self.executor.start()
        
        # Your MQTT connection here
    
    def on_mqtt_message(self, topic, payload, qos):
        self.executor.submit(topic, payload, your_handler)

# Run your app
async def main():
    app = YourMQTTApplication()
    await app.start()
    # Keep running...
```

---

## 🎯 What Metrics Are Tracked?

Every 2 seconds, your callback receives:

```python
{
    'cpu': 3.5,        # CPU EWMA percentage
    'mem': 74.2,       # Memory usage percentage
    'qsize': 450,      # Current queue size
    'qusage': 0.225,   # Queue usage (0.0 to 1.0)
    'workers': 6       # Active worker count
}
```

---

## 🚨 Important: No Separate Process Needed!

### ❌ You DON'T need to run:
```bash
# DON'T DO THIS in production
poetry run python run_demo_with_prometheus.py
```

### ✅ You DO need to:
```python
# Just integrate in your MQTT app
from smart_scaling_executor import SmartScalingExecutor

self.executor = SmartScalingExecutor(metrics_cb=callback)
await self.executor.start()
```

**The metrics are tracked automatically within your application!**

---

## 📊 Visualizing Metrics (Optional)

### **For Development:**
- Use logging (Option 1)
- Check logs/console

### **For Production:**
- Use Prometheus (Option 2)
- Setup Grafana dashboard
- Configure alerts

### **Grafana Setup:**
1. Install Prometheus
2. Configure to scrape `http://your-app:8000/metrics`
3. Install Grafana
4. Add Prometheus as data source
5. Create dashboard with panels:
   - `smart_executor_queue_size`
   - `smart_executor_worker_count`
   - `smart_executor_cpu_ewma_percent`
   - `smart_executor_memory_usage_percent`
   - `smart_executor_queue_usage_percent`

See: `PROMETHEUS_GRAFANA_SETUP.md` for detailed steps

---

## 🎯 Quick Decision Guide

**Choose Simple Logging if:**
- ✅ You just want basic visibility
- ✅ Development/testing environment
- ✅ Don't need fancy dashboards
- ✅ Logs are sufficient

**Choose Prometheus if:**
- ✅ Production environment
- ✅ Need dashboards and alerts
- ✅ Want historical metrics
- ✅ Multiple services to monitor

---

## 📝 Summary

### **Key Points:**
1. ✅ **No separate process needed** - integrate directly
2. ✅ **Metrics tracked automatically** - every 2 seconds
3. ✅ **Two options** - simple logging or Prometheus
4. ✅ **Use metrics_cb parameter** - that's it!
5. ✅ **All in your application** - no external scripts

### **Files to Reference:**
- `production_integration_simple.py` - Complete simple example
- `production_integration_prometheus.py` - Complete Prometheus example
- `smart_scaling_executor.py` - The executor itself
- `prometheus_metrics.py` - Prometheus utilities

### **Your Next Step:**
1. Copy `smart_scaling_executor.py` to your project
2. Choose Option 1 (simple) or Option 2 (Prometheus)
3. Follow the integration example
4. Deploy and monitor!

---

**Questions?**
- Check the example files
- See `PROMETHEUS_GRAFANA_SETUP.md` for Grafana setup
- All code is ready to copy-paste!

**You're ready for production!** 🚀

