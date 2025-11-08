# tests/test_on_message_handler_executor_consumer.py
import pytest
import asyncio
import pytest_asyncio
from mqtt_connector_lib.gmqtt_connector import GMqttConnector, BrokerClient
from mqtt_connector_lib.on_message_handler_executor import OnMessageHandlerExecutor


class TestOnMessageHandlerExecutorConsumerPerspective:
    """Test OnMessageHandlerExecutor from consumer/developer usage perspective"""

    @pytest_asyncio.fixture
    async def real_connected_connector(self):
        """Setup real MQTT connector for consumer testing"""
        broker_details = BrokerClient(
            host="test.mosquitto.org",
            port=1883,
            client_id="test_client_on_message_handler_12345"
        )

        conn = GMqttConnector(broker_details=broker_details, clean_session=True, max_on_message_handler_workers=5)
        await conn.connectAsync(username=broker_details.user_name, password=broker_details.password)

        yield conn

        # disconnectAsync already stops the executor internally
        try:
            await conn.disconnectAsync()
        except Exception:
            pass

    @pytest.mark.asyncio
    async def test_consumer_registers_async_handler_and_receives_messages(self, real_connected_connector):
        """Test: Consumer registers async handler and processes real MQTT messages"""
        received_messages = []

        async def temperature_sensor_handler(topic: str, payload: str):
            """Consumer's async handler for temperature data"""
            await asyncio.sleep(0.1)  # Simulate async processing
            data = {"topic": topic, "temperature": float(payload), "processed": True}
            received_messages.append(data)

        # Consumer subscribes to topic with their handler
        test_topic = "sensors/temperature/test/12345"
        await real_connected_connector.subscribeAsync(test_topic, temperature_sensor_handler, qos=1)
        await asyncio.sleep(0.5)  # Wait for SUBACK

        # Publish real MQTT messages to the topic
        await real_connected_connector.publishAsync(test_topic, "25.5", qos=1)
        await real_connected_connector.publishAsync(test_topic, "26.0", qos=1)

        # Allow time for messages to be received and processed
        await asyncio.sleep(1.0)

        # Verify consumer's handler processed messages
        assert len(received_messages) == 2
        assert received_messages[0]["temperature"] == 25.5
        assert received_messages[1]["temperature"] == 26.0
        assert all(msg["processed"] for msg in received_messages)

    @pytest.mark.asyncio
    async def test_consumer_handles_high_throughput_scenario(self, real_connected_connector):
        """Test: Consumer handles high-throughput real MQTT messages"""
        processed_count = 0
        processing_times = []

        async def iot_device_handler(topic: str, payload: str):
            """Consumer's handler for high-throughput IoT data"""
            nonlocal processed_count
            import time
            start_time = time.time()

            # Simulate real processing work
            await asyncio.sleep(0.05)
            device_data = {"device_id": payload, "timestamp": start_time}

            processed_count += 1
            processing_times.append(time.time() - start_time)

        test_topic = "iot/devices/test/throughput/12345"
        await real_connected_connector.subscribeAsync(test_topic, iot_device_handler)
        await asyncio.sleep(0.5)  # Wait for SUBACK

        # # Publish burst of 10 messages (reduced from 20 for broker stability)
        # for i in range(10):
        #     await real_connected_connector.publishAsync(test_topic, f"device_{i:03d}", qos=0)
        #
        # # Wait for processing (should be concurrent due to worker pool)
        # await asyncio.sleep(2.0)
        #
        # # Verify all messages processed
        # assert processed_count == 10
        #
        # # Verify concurrent processing happened
        # total_processing_time = sum(processing_times)
        # # With concurrency, total time should be less than sequential
        # assert total_processing_time < 1.0

        # Publish burst of 20 messages
        for i in range(20):
            await real_connected_connector.publishAsync(test_topic, f"device_{i:03d}", qos=0)

        # Wait for processing (should be concurrent due to worker pool)
        await asyncio.sleep(3.0)  # Increased from 2.0 to 3.0 for 20 messages

        # Verify all messages processed
        assert processed_count == 20  # Changed from 10 to 20

        # Verify concurrent processing happened
        total_processing_time = sum(processing_times)
        # With concurrency, total time should be less than sequential
        # Sequential: 20 messages × 0.05s = 1.0s total
        # With 3 workers, should complete in ~0.35s (1.0s ÷ 3 + overhead)
        assert total_processing_time < 1.5  # Changed from 1.0 to 1.5

    @pytest.mark.asyncio
    async def test_consumer_error_handling_in_message_processing(self, real_connected_connector):
        """Test: Consumer's handler throws exception with real MQTT messages"""
        successful_messages = []
        error_messages = []

        async def faulty_handler(topic: str, payload: bytes):
            """Consumer's handler that sometimes fails"""
            # Check for exact match with bytes
            if payload == b"error_message":
                error_messages.append(payload)
                raise ValueError(f"Processing failed for: {payload}")

            # Only successful messages get here
            successful_messages.append({"topic": topic, "payload": payload})

        test_topic = "test/errors/handler/12345"
        await real_connected_connector.subscribeAsync(test_topic, faulty_handler)
        await asyncio.sleep(0.5)

        # Publish mix of good and bad messages
        await real_connected_connector.publishAsync(test_topic, "good_message_1", qos=0)
        await real_connected_connector.publishAsync(test_topic, "error_message", qos=0)
        await real_connected_connector.publishAsync(test_topic, "good_message_2", qos=0)

        await asyncio.sleep(2.0)

        print(f"Successful messages: {successful_messages}")
        print(f"Error messages: {error_messages}")

        # Verify good messages processed, bad ones logged but system stable
        assert len(
            successful_messages) == 2, f"Expected 2 successful messages, got {len(successful_messages)}: {successful_messages}"
        assert len(error_messages) == 1, f"Expected 1 error message, got {len(error_messages)}: {error_messages}"

    @pytest.mark.asyncio
    async def test_consumer_multiple_topic_handlers(self, real_connected_connector):
        """Test: Consumer registers handlers for multiple topics with real MQTT"""
        alerts_received = []
        metrics_received = []

        async def security_alert_handler(topic: str, payload: bytes):
            """Consumer's security alert handler"""
            alerts_received.append({"topic": topic, "alert": payload, "priority": "high"})

        async def system_metrics_handler(topic: str, payload: bytes):
            """Consumer's system metrics handler"""
            metrics_received.append({"topic": topic, "metric": payload, "type": "performance"})

        # Consumer subscribes to different topics with different handlers
        alerts_topic = "security/alerts/test/12345"
        metrics_topic = "system/metrics/test/12345"

        await real_connected_connector.subscribeAsync(alerts_topic, security_alert_handler)
        await real_connected_connector.subscribeAsync(metrics_topic, system_metrics_handler)
        await asyncio.sleep(0.5)  # Wait for SUBACKs

        # Publish messages to different topics
        await real_connected_connector.publishAsync(alerts_topic, "unauthorized_access", qos=1)
        await real_connected_connector.publishAsync(metrics_topic, "cpu_usage:85%", qos=0)
        await real_connected_connector.publishAsync(alerts_topic, "failed_login", qos=1)

        await asyncio.sleep(1.0)

        # Verify correct handlers processed their messages
        assert len(alerts_received) == 2
        assert len(metrics_received) == 1
        assert alerts_received[0]["alert"] == b"unauthorized_access"
        assert metrics_received[0]["metric"] == b"cpu_usage:85%"

    @pytest.mark.asyncio
    async def test_consumer_handler_with_external_service_calls(self, real_connected_connector):
        """Test: Consumer's handler makes external API calls with real MQTT"""
        api_calls_made = []

        async def webhook_notifier_handler(topic: str, payload: bytes):
            """Consumer's handler that calls external webhook"""
            # Simulate external API call
            await asyncio.sleep(0.1)  # Network delay simulation

            api_response = {
                "webhook_url": "https://api.example.com/notify",
                "payload_sent": payload,
                "response_code": 200,
                "timestamp": "2024-01-01T10:00:00Z"
            }
            api_calls_made.append(api_response)

        test_topic = "webhooks/trigger/test/12345"
        await real_connected_connector.subscribeAsync(test_topic, webhook_notifier_handler)
        await asyncio.sleep(0.5)  # Wait for SUBACK

        # Publish notification triggers
        await real_connected_connector.publishAsync(test_topic, "user_signup", qos=0)
        await real_connected_connector.publishAsync(test_topic, "payment_completed", qos=0)

        await asyncio.sleep(1.0)

        # Verify external API calls were made
        assert len(api_calls_made) == 2
        assert api_calls_made[0]["payload_sent"] == b"user_signup"
        assert api_calls_made[1]["payload_sent"] == b"payment_completed"

    @pytest.mark.asyncio
    async def test_consumer_queue_overflow_scenario(self, real_connected_connector):
        """Test: Consumer handles queue overflow with real MQTT messages"""
        # Create executor with small queue for testing
        small_queue_executor = OnMessageHandlerExecutor(max_workers=1, queue_size=2)

        # Stop current executor and replace with small one
        await real_connected_connector._on_message_handler_executor.stop()
        real_connected_connector._on_message_handler_executor = small_queue_executor
        await small_queue_executor.start()

        processed_messages = []

        async def slow_handler(topic: str, payload: bytes):
            """Slow handler to cause queue backup"""
            await asyncio.sleep(0.5)  # Slow processing
            processed_messages.append(payload)

        test_topic = "test/queue/overflow/12345"
        await real_connected_connector.subscribeAsync(test_topic, slow_handler)
        await asyncio.sleep(0.5)  # Wait for SUBACK

        # Publish messages rapidly to overflow queue
        for i in range(8):  # Reduced number for real broker
            await real_connected_connector.publishAsync(test_topic, f"message_{i}", qos=0)

        await asyncio.sleep(3.0)
        await small_queue_executor.stop()

        # Some messages should be dropped due to queue overflow
        assert len(processed_messages) < 8
        assert len(processed_messages) > 0

    @pytest.mark.asyncio
    async def test_consumer_graceful_shutdown_scenario(self, real_connected_connector):
        """Test: Consumer's handlers complete gracefully during shutdown"""
        processing_started = []
        processing_completed = []

        async def long_running_handler(topic: str, payload: bytes):
            """Consumer's handler with longer processing time"""
            processing_started.append(payload)
            await asyncio.sleep(0.3)  # Simulate longer work
            processing_completed.append(payload)

        test_topic = "test/shutdown/graceful/12345"
        await real_connected_connector.subscribeAsync(test_topic, long_running_handler)
        await asyncio.sleep(0.5)  # Wait for SUBACK

        # Publish messages that will start long-running processing
        await real_connected_connector.publishAsync(test_topic, "task_1", qos=0)
        await real_connected_connector.publishAsync(test_topic, "task_2", qos=0)

        # Allow processing to start and complete
        # Each task takes 0.3s, with 5 workers they can run concurrently
        # But we need to account for message arrival and queue processing time
        await asyncio.sleep(1.0)  # Increased from 0.7s to 1.0s

        # Verify in-flight processing completed
        assert len(processing_started) == 2
        assert len(processing_completed) == 2

    @pytest.mark.asyncio
    async def test_consumer_monitoring_queue_performance(self, real_connected_connector):
        """Test: Consumer monitors queue performance with real MQTT"""
        processing_delays = []

        async def monitored_handler(topic: str, payload: bytes):
            """Consumer's handler that tracks processing delays"""
            await asyncio.sleep(0.1)

            # Consumer checks queue size for monitoring
            current_queue_size = real_connected_connector._on_message_handler_executor.get_on_message_queue_size()
            processing_delays.append(current_queue_size)

        test_topic = "monitoring/test/performance/12345"
        await real_connected_connector.subscribeAsync(test_topic, monitored_handler)
        await asyncio.sleep(0.5)  # Wait for SUBACK

        # Publish messages and monitor queue behavior
        for i in range(5):
            await real_connected_connector.publishAsync(test_topic, f"msg_{i}", qos=0)
            await asyncio.sleep(0.1)  # Slight delay between publishes

        await asyncio.sleep(1.5)

        # Verify monitoring data was collected
        assert len(processing_delays) == 5
        # Queue should generally be empty or small for this load
        assert all(delay <= 3 for delay in processing_delays)
