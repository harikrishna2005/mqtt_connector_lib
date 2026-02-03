# Prometheus + Grafana Setup Guide

This guide explains how to set up Prometheus and Grafana to visualize metrics from SmartScalingExecutor.

## Architecture Overview

```
SmartScalingExecutor (Your App)
    ↓ exposes metrics on HTTP
    ↓ http://localhost:8000/metrics
Prometheus (Time-Series Database)
    ↓ stores metrics over time
    ↓ provides PromQL query language
Grafana (Visualization Dashboard)
    ↓ creates beautiful charts
    ↓ provides alerting
Your Browser
```

## Quick Comparison

| Tool | Purpose | Port | What it Does |
|------|---------|------|--------------|
| **Your App** | Generates metrics | 8000 | Exposes `/metrics` endpoint |
| **Prometheus** | Collects & stores metrics | 9090 | Scrapes metrics, stores time-series data |
| **Grafana** | Visualizes metrics | 3000 | Creates dashboards, charts, alerts |

## Prerequisites

```bash
# Install prometheus-client in your Python environment
poetry add prometheus-client

# OR with pip
pip install prometheus-client
```

## Step 1: Run Your Application with Prometheus Metrics

```bash
cd Examples/POC/smart_executor_live
python run_demo_with_prometheus.py
```

You should see:
```
✅ Prometheus metrics available at: http://localhost:8000/metrics
```

Visit http://localhost:8000/metrics in your browser to verify metrics are exposed.

Example output:
```
# HELP smart_executor_queue_size Current number of messages in the executor queue
# TYPE smart_executor_queue_size gauge
smart_executor_queue_size 45.0
# HELP smart_executor_worker_count Number of active worker tasks
# TYPE smart_executor_worker_count gauge
smart_executor_worker_count 5.0
# HELP smart_executor_cpu_ewma_percent CPU usage exponentially weighted moving average (0-100)
# TYPE smart_executor_cpu_ewma_percent gauge
smart_executor_cpu_ewma_percent 23.5
```

## Step 2: Install Prometheus

### Windows:
```powershell
# Download from https://prometheus.io/download/
# Extract to C:\prometheus
# Or use Chocolatey:
choco install prometheus
```

### Linux:
```bash
# Download
wget https://github.com/prometheus/prometheus/releases/download/v2.45.0/prometheus-2.45.0.linux-amd64.tar.gz
tar xvfz prometheus-*.tar.gz
cd prometheus-*
```

### Docker (Easiest):
```bash
docker run -d --name=prometheus -p 9090:9090 -v $(pwd)/prometheus.yml:/etc/prometheus/prometheus.yml prom/prometheus
```

## Step 3: Configure Prometheus

Create `prometheus.yml` in your directory:

```yaml
global:
  scrape_interval: 5s  # How often to scrape metrics
  evaluation_interval: 5s

scrape_configs:
  - job_name: 'smart_executor'
    static_configs:
      - targets: ['localhost:8000']  # Your app's metrics endpoint
        labels:
          instance: 'smart_executor_demo'
          env: 'dev'
```

## Step 4: Start Prometheus

```bash
# If installed locally
prometheus --config.file=prometheus.yml

# If using Docker
docker run -d \
  --name=prometheus \
  -p 9090:9090 \
  -v $(pwd)/prometheus.yml:/etc/prometheus/prometheus.yml \
  prom/prometheus
```

Visit http://localhost:9090 to access Prometheus UI.

Verify your target is being scraped:
- Go to http://localhost:9090/targets
- You should see `smart_executor` with state "UP"

## Step 5: Query Metrics in Prometheus

Go to http://localhost:9090/graph and try these queries:

```promql
# Current queue size
smart_executor_queue_size

# Queue usage percentage
smart_executor_queue_usage_percent

# Worker count over time
smart_executor_worker_count

# CPU usage
smart_executor_cpu_ewma_percent

# Rate of queue growth (per second)
rate(smart_executor_queue_size[1m])

# Average worker count over 5 minutes
avg_over_time(smart_executor_worker_count[5m])
```

## Step 6: Install Grafana

### Windows:
```powershell
# Download from https://grafana.com/grafana/download
# Or use Chocolatey:
choco install grafana
```

### Docker (Easiest):
```bash
docker run -d --name=grafana -p 3000:3000 grafana/grafana
```

### Linux:
```bash
sudo apt-get install -y adduser libfontconfig1
wget https://dl.grafana.com/oss/release/grafana_10.0.0_amd64.deb
sudo dpkg -i grafana_10.0.0_amd64.deb
sudo systemctl start grafana-server
```

## Step 7: Configure Grafana

1. **Access Grafana**: http://localhost:3000
   - Default username: `admin`
   - Default password: `admin`

2. **Add Prometheus Data Source**:
   - Click "⚙️ Configuration" → "Data Sources"
   - Click "Add data source"
   - Select "Prometheus"
   - Set URL: `http://localhost:9090`
   - Click "Save & Test"

3. **Create Dashboard**:
   - Click "+" → "Dashboard"
   - Click "Add new panel"

## Step 8: Create Grafana Panels

### Panel 1: Queue Size Over Time
- **Query**: `smart_executor_queue_size`
- **Visualization**: Time series (line chart)
- **Title**: "Queue Size"
- **Y-axis**: Messages

### Panel 2: Worker Count
- **Query**: `smart_executor_worker_count`
- **Visualization**: Time series
- **Title**: "Active Workers"
- **Y-axis**: Workers

### Panel 3: CPU Usage (EWMA)
- **Query**: `smart_executor_cpu_ewma_percent`
- **Visualization**: Gauge or Time series
- **Title**: "CPU Usage (EWMA)"
- **Y-axis**: Percentage (0-100)
- **Thresholds**: 
  - Green: 0-60%
  - Yellow: 60-80%
  - Red: 80-100%

### Panel 4: Queue Usage Percentage
- **Query**: `smart_executor_queue_usage_percent`
- **Visualization**: Gauge
- **Title**: "Queue Usage"
- **Y-axis**: Percentage (0-100)
- **Thresholds**:
  - Green: 0-70%
  - Yellow: 70-85%
  - Red: 85-100%

### Panel 5: Memory Usage
- **Query**: `smart_executor_memory_usage_percent`
- **Visualization**: Time series
- **Title**: "Memory Usage"
- **Y-axis**: Percentage (0-100)

## Step 9: Advanced Grafana Features

### Alerts
Create alerts for critical conditions:

1. **High Queue Usage Alert**:
   - Query: `smart_executor_queue_usage_percent > 85`
   - Condition: Alert when above threshold for > 1 minute
   - Notification: Email, Slack, PagerDuty, etc.

2. **Worker Scaling Alert**:
   - Query: `smart_executor_worker_count >= 25`
   - Condition: Approaching max workers
   - Action: Notify operations team

### Variables
Create dashboard variables for dynamic queries:
- `$instance`: Select which executor instance
- `$env`: Filter by environment (dev, staging, prod)

### Annotations
Add event markers to charts:
- Deployment times
- Configuration changes
- Burst load events

## Complete Docker Compose Setup

Create `docker-compose.yml`:

```yaml
version: '3.8'

services:
  prometheus:
    image: prom/prometheus:latest
    container_name: prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus-data:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
    networks:
      - monitoring

  grafana:
    image: grafana/grafana:latest
    container_name: grafana
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
      - GF_USERS_ALLOW_SIGN_UP=false
    volumes:
      - grafana-data:/var/lib/grafana
    networks:
      - monitoring
    depends_on:
      - prometheus

networks:
  monitoring:
    driver: bridge

volumes:
  prometheus-data:
  grafana-data:
```

Start everything:
```bash
docker-compose up -d
```

## Verification Checklist

- [ ] Your app exposes metrics: http://localhost:8000/metrics ✅
- [ ] Prometheus scrapes your app: http://localhost:9090/targets ✅
- [ ] Prometheus can query metrics: http://localhost:9090/graph ✅
- [ ] Grafana connects to Prometheus: http://localhost:3000 ✅
- [ ] Dashboard displays live metrics ✅

## Troubleshooting

### Prometheus can't scrape your app
```bash
# Check if your app is running
curl http://localhost:8000/metrics

# Check Prometheus targets
# Visit http://localhost:9090/targets
# Ensure target shows as "UP"
```

### Metrics not showing in Grafana
1. Verify Prometheus data source is configured correctly
2. Check query syntax in Grafana panel
3. Ensure time range includes recent data
4. Verify Prometheus is actually collecting data

### Port conflicts
```bash
# If port 8000 is in use, change it:
await start_prometheus_server(port=8001)

# Update prometheus.yml accordingly
```

## Key Differences: Current vs Prometheus Setup

| Feature | Current (CSV/Plotly) | With Prometheus/Grafana |
|---------|---------------------|-------------------------|
| **Data Storage** | CSV file | Time-series database |
| **Visualization** | Static plots | Live dashboards |
| **Real-time** | Manual refresh | Auto-refresh (5s) |
| **Alerting** | None | Built-in alerts |
| **Multiple Instances** | Difficult | Easy (labels) |
| **Historical Data** | CSV limited | Efficient storage |
| **Query Language** | None | PromQL |
| **Production Ready** | No | Yes |

## Next Steps for Production

1. **Add more metrics**:
   - Message processing latency
   - Error rates
   - Handler execution time

2. **Set up alerting**:
   - High queue usage
   - Worker exhaustion
   - CPU overload

3. **Use service discovery**:
   - Auto-detect multiple instances
   - Kubernetes integration

4. **Secure endpoints**:
   - Add authentication to metrics endpoint
   - Use HTTPS

## Summary

**Your current code does NOT emit Prometheus metrics yet.** 

To implement the future improvement:
1. ✅ I created `prometheus_metrics.py` (Prometheus support)
2. ✅ I created `run_demo_with_prometheus.py` (Example usage)
3. ⏳ You need to install prometheus-client: `poetry add prometheus-client`
4. ⏳ You need to install and configure Prometheus
5. ⏳ You need to install and configure Grafana

Run `run_demo_with_prometheus.py` to see it in action!

