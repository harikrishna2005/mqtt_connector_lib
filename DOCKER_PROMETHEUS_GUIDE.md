# 🐳 Docker Deployment with Prometheus Metrics + Terminal Dashboard

## 🎯 What You Get

When running in Docker, you get **BOTH**:
1. **Prometheus metrics** at `http://localhost:8000/metrics` (for Grafana)
2. **Terminal dashboard** in Docker logs (real-time visualization)

---

## 🚀 Quick Start

### Step 1: Update Your Application Code

```python
import asyncio
from mqtt_connector_lib.gmqtt_connector import GMqttConnector, BrokerClient, set_metrics_callback
from mqtt_connector_lib.prometheus_metrics import start_prometheus_server, create_combined_callback


async def your_message_handler(topic: str, payload: bytes):
    """Your actual message handler"""
    # Process message
    pass


async def main():
    # 1. Start Prometheus server
    await start_prometheus_server(port=8000)
    
    # 2. Configure combined callback (Prometheus + Terminal Dashboard)
    callback = create_combined_callback(enable_terminal_dashboard=True)
    set_metrics_callback(callback)
    
    # 3. Create MQTT connector (metrics will be tracked automatically)
    broker = BrokerClient(
        host="your-broker.com",
        port=1883,
        client_id="your_client_id"
    )
    
    mqtt_client = GMqttConnector(broker_details=broker)
    
    # 4. Connect and use normally
    await mqtt_client.connectAsync()
    await mqtt_client.subscribeAsync("your/topic", handler=your_message_handler)
    
    # Your application logic...
    await asyncio.sleep(3600)  # Run for 1 hour


if __name__ == "__main__":
    asyncio.run(main())
```

---

## 🐳 Docker Setup

### Option 1: Dockerfile (Recommended)

```dockerfile
FROM python:3.12-slim

WORKDIR /app

# Copy your application
COPY . .

# Install dependencies
RUN pip install poetry
RUN poetry install --no-dev

# Expose Prometheus port
EXPOSE 8000

# Run your application
CMD ["poetry", "run", "python", "your_app.py"]
```

### Build and Run:

```bash
# Build
docker build -t your-mqtt-app .

# Run (expose Prometheus port)
docker run -p 8000:8000 your-mqtt-app

# View logs (see terminal dashboard)
docker logs -f <container-id>
```

---

## 📊 Viewing Metrics

### In Docker Logs (Terminal Dashboard)

```bash
# Follow logs to see real-time terminal dashboard
docker logs -f <container-id>
```

**You'll see:**
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

⏱️  Last updated: 14:35:22
📊 Prometheus metrics: http://localhost:8000/metrics
================================================================================
```

### In Browser (Prometheus Metrics)

```bash
# Open in browser
http://localhost:8000/metrics
```

**You'll see:**
```
smart_executor_worker_count 7
smart_executor_queue_size 350
smart_executor_cpu_ewma_percent 12.5
smart_executor_memory_usage_percent 68.2
smart_executor_queue_usage_percent 17.5
```

---

## 🎛️ Configuration Options

### Option 1: Prometheus + Terminal Dashboard (Default)

**Best for: Development, debugging, Docker logs monitoring**

```python
callback = create_combined_callback(enable_terminal_dashboard=True)
set_metrics_callback(callback)
```

**Features:**
- ✅ Prometheus metrics at :8000/metrics
- ✅ Live terminal dashboard in logs
- ✅ Worker and queue trends
- ✅ Real-time updates

---

### Option 2: Prometheus Only

**Best for: Production with Grafana, no terminal output needed**

```python
from mqtt_connector_lib.prometheus_metrics import prometheus_metrics_callback

set_metrics_callback(prometheus_metrics_callback)
```

**Features:**
- ✅ Prometheus metrics at :8000/metrics
- ❌ No terminal dashboard

---

### Option 3: Terminal Dashboard Only

**Best for: Local development without Prometheus**

```python
from mqtt_connector_lib.prometheus_metrics import TerminalDashboard

dashboard = TerminalDashboard()
set_metrics_callback(dashboard.display)
```

**Features:**
- ❌ No Prometheus metrics
- ✅ Live terminal dashboard

---

## 🔧 Docker Compose Example

```yaml
version: '3.8'

services:
  mqtt-app:
    build: .
    ports:
      - "8000:8000"  # Prometheus metrics
    environment:
      - MQTT_BROKER_HOST=broker.example.com
      - MQTT_BROKER_PORT=1883
    restart: unless-stopped
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"
  
  # Optional: Grafana
  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    depends_on:
      - mqtt-app
  
  # Optional: Prometheus
  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
    depends_on:
      - mqtt-app
```

**prometheus.yml:**
```yaml
global:
  scrape_interval: 5s

scrape_configs:
  - job_name: 'smart-executor'
    static_configs:
      - targets: ['mqtt-app:8000']
```

---

## 📈 Grafana Dashboard (Optional)

### 1. Start Grafana (Docker Compose)

```bash
docker-compose up -d grafana
```

### 2. Add Prometheus Data Source

- Go to: http://localhost:3000
- Login: admin/admin
- Add data source: Prometheus
- URL: http://prometheus:9090

### 3. Create Dashboard

**Queries to use:**
```promql
# Worker count
smart_executor_worker_count

# Queue size
smart_executor_queue_size

# CPU usage
smart_executor_cpu_ewma_percent

# Memory usage
smart_executor_memory_usage_percent

# Queue usage %
smart_executor_queue_usage_percent
```

---

## 🎯 Best Practices for Docker

### 1. Always Expose Prometheus Port

```dockerfile
EXPOSE 8000
```

```bash
docker run -p 8000:8000 your-app
```

### 2. Enable Terminal Dashboard in Logs

```python
# This shows in `docker logs`
callback = create_combined_callback(enable_terminal_dashboard=True)
```

### 3. Use Docker Logging

```bash
# View real-time
docker logs -f <container>

# View last 100 lines
docker logs --tail 100 <container>

# Since specific time
docker logs --since 5m <container>
```

### 4. Health Checks

```dockerfile
HEALTHCHECK --interval=30s --timeout=3s \
  CMD curl -f http://localhost:8000/metrics || exit 1
```

---

## 🐛 Troubleshooting

### Problem: "Address already in use"

**Solution:** Prometheus server already running

```python
# The library handles this gracefully - it logs a warning but continues
await start_prometheus_server(port=8000)
```

### Problem: Terminal dashboard not showing in logs

**Solution:** Ensure you're using the combined callback

```python
callback = create_combined_callback(enable_terminal_dashboard=True)
set_metrics_callback(callback)
```

### Problem: Metrics not updating

**Solution:** Verify SmartScalingExecutor is receiving the callback

```python
# Check in logs for:
# "Metrics callback configured: Prometheus + Terminal Dashboard"
```

---

## 📊 Metrics Summary

| Metric | Description | Type |
|--------|-------------|------|
| `smart_executor_worker_count` | Current number of workers | Gauge |
| `smart_executor_queue_size` | Current queue size | Gauge |
| `smart_executor_cpu_ewma_percent` | CPU usage (EWMA) | Gauge |
| `smart_executor_memory_usage_percent` | Memory usage | Gauge |
| `smart_executor_queue_usage_percent` | Queue usage (0-100%) | Gauge |

---

## ✨ Complete Example

```python
import asyncio
from mqtt_connector_lib.gmqtt_connector import GMqttConnector, BrokerClient, set_metrics_callback
from mqtt_connector_lib.prometheus_metrics import start_prometheus_server, create_combined_callback


async def handler(topic, payload):
    await asyncio.sleep(0.1)


async def main():
    # Setup metrics
    await start_prometheus_server(port=8000)
    callback = create_combined_callback(enable_terminal_dashboard=True)
    set_metrics_callback(callback)
    
    # Create MQTT client
    broker = BrokerClient(
        host="test.mosquitto.org",
        port=1883,
        client_id="docker_demo"
    )
    
    mqtt = GMqttConnector(broker_details=broker)
    await mqtt.connectAsync()
    await mqtt.subscribeAsync("test/#", handler=handler)
    
    # Run forever
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        await mqtt.disconnectAsync()


if __name__ == "__main__":
    asyncio.run(main())
```

**Run in Docker:**
```bash
docker run -p 8000:8000 your-app
docker logs -f <container>  # See terminal dashboard
```

**View metrics:**
- Terminal: `docker logs -f <container>`
- Prometheus: http://localhost:8000/metrics
- Grafana: http://localhost:3000

---

**You get the best of both worlds: Terminal dashboard for debugging + Prometheus for production monitoring!** 🎉

