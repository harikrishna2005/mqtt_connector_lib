"""
Example: Using Prometheus metrics with GMqttConnector and Terminal Dashboard

This shows how to:
1. Start Prometheus server (for Grafana/scraping)
2. Enable terminal dashboard (for real-time viewing)
3. Use SmartScalingExecutor with metrics

Perfect for Docker deployments where you want BOTH:
- Prometheus metrics (for Grafana)
- Terminal output (for logs/debugging)
"""

import asyncio
from mqtt_connector_lib.gmqtt_connector import GMqttConnector, BrokerClient, set_metrics_callback
from mqtt_connector_lib.prometheus_metrics import start_prometheus_server, create_combined_callback


async def sample_handler(topic: str, payload: bytes):
    """Example message handler"""
    # print(f"Received: {topic} -> {payload.decode()}")
    await asyncio.sleep(0.1)  # Simulate processing


async def main():
    print("🚀 Starting MQTT Connector with Prometheus Metrics & Terminal Dashboard")
    print("=" * 80)

    # ============================================================================
    # Step 1: Start Prometheus Server (for Grafana/metrics scraping)
    # ============================================================================
    await start_prometheus_server(port=8000)
    print()

    # ============================================================================
    # Step 2: Create Combined Callback (Prometheus + Terminal Dashboard)
    # ============================================================================
    # Option A: With terminal dashboard (recommended for Docker with log viewing)
    callback = create_combined_callback(enable_terminal_dashboard=True)

    # Option B: Prometheus only (if you don't want terminal output)
    # from mqtt_connector_lib.prometheus_metrics import prometheus_metrics_callback
    # callback = prometheus_metrics_callback

    set_metrics_callback(callback)
    print("✅ Metrics callback configured: Prometheus + Terminal Dashboard")
    print()

    # ============================================================================
    # Step 3: Create MQTT Connector (SmartScalingExecutor will use metrics)
    # ============================================================================
    broker_details = BrokerClient(
        host="test.mosquitto.org",
        port=1883,
        client_id="smart_scaling_demo_client"
    )

    mqtt_client = GMqttConnector(
        broker_details=broker_details,
        clean_session=True
    )

    print("✅ MQTT Connector created with SmartScalingExecutor")
    print()

    # ============================================================================
    # Step 4: Connect to Broker
    # ============================================================================
    print("📡 Connecting to MQTT broker...")
    await mqtt_client.connectAsync()
    print("✅ Connected to broker")
    print()

    # ============================================================================
    # Step 5: Subscribe to Topics
    # ============================================================================
    print("📥 Subscribing to topics...")
    await mqtt_client.subscribeAsync("test/smart_scaling/#", handler=sample_handler, qos=0)
    print("✅ Subscribed to test/smart_scaling/#")
    print()

    # ============================================================================
    # Step 6: Publish Test Messages (to generate load)
    # ============================================================================
    print("📤 Publishing test messages to generate load...")
    print()

    async def publish_load():
        """Generate test load"""
        for i in range(100):
            await mqtt_client.publishAsync(
                topic="test/smart_scaling/messages",
                payload=f"Message {i}",
                qos=0
            )
            await asyncio.sleep(0.05)  # 20 messages/sec

    # Start publishing in background
    publish_task = asyncio.create_task(publish_load())

    # ============================================================================
    # Step 7: Run and Display Metrics
    # ============================================================================
    print("=" * 80)
    print("📊 MONITORING:")
    print("   - Terminal Dashboard: See below (updates every 2 seconds)")
    print("   - Prometheus Metrics: http://localhost:8000/metrics")
    print("   - Grafana: Configure to scrape http://localhost:8000/metrics")
    print()
    print("Press Ctrl+C to stop")
    print("=" * 80)
    print()

    try:
        # Wait for publish to complete
        await publish_task

        # Keep running to see metrics
        print("\n✅ Test messages published. Monitoring metrics...")
        print("   (Worker scaling will adjust based on load)\n")

        # Run for 60 seconds to see scaling in action
        await asyncio.sleep(60)

    except KeyboardInterrupt:
        print("\n\n⏹️  Stopping...")
    finally:
        # ========================================================================
        # Step 8: Cleanup
        # ========================================================================
        print("\n🧹 Cleaning up...")
        await mqtt_client.unsubscribeAsync("test/smart_scaling/#")
        await mqtt_client.disconnectAsync()
        print("✅ Disconnected")
        print()
        print("=" * 80)
        print("📊 Final Summary:")
        print("   - Prometheus metrics were available at: http://localhost:8000/metrics")
        print("   - Terminal dashboard showed real-time worker/queue stats")
        print("   - SmartScalingExecutor automatically adjusted workers based on load")
        print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())

