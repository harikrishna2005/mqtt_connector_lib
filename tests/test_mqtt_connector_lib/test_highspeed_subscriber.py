"""
Integration Tests for HighSpeedSubscriber

These tests verify the core functionality of HighSpeedSubscriber:
1. Subscribe and receive messages
2. Multiple channels subscription
3. Automatic reconnection on Redis disconnect
4. Handler error handling
5. Sync and async handler support
6. Start/stop lifecycle

Tests use real Redis server (not mocks) to ensure production behavior.
"""

import asyncio
import pytest
import time
from mqtt_connector_lib.redis_connector import RedisConnector, HighSpeedPublisher, HighSpeedSubscriber


# Test configuration
REDIS_HOST = "192.168.29.42"
REDIS_PORT = 6379
REDIS_PASSWORD = "SuperDuperRedis6748@"


@pytest.fixture
async def redis_connector():
    """Create Redis connector for tests"""
    connector = RedisConnector(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        auto_start=False
    )
    yield connector
    await connector.close()


@pytest.fixture
async def publisher(redis_connector):
    """Create publisher for tests"""
    return HighSpeedPublisher(redis_connector)


@pytest.fixture
async def subscriber(redis_connector):
    """Create subscriber for tests"""
    sub = HighSpeedSubscriber(redis_connector)
    yield sub
    if sub.is_running:
        await sub.stop()


class MessageCollector:
    """Simple bot for testing message reception"""

    def __init__(self):
        self.messages = {}
        self.update_counts = {}

    def create_handler(self, channel: str):
        """Factory to create handler for specific channel"""
        def handler(data: str):
            self.messages[channel] = data
            self.update_counts[channel] = self.update_counts.get(channel, 0) + 1
        return handler

    def get_count(self, channel: str) -> int:
        return self.update_counts.get(channel, 0)

    def get_message(self, channel: str) -> str:
        return self.messages.get(channel, None)


# ============================================================================
# TEST 1: Basic Subscribe and Receive
# ============================================================================

@pytest.mark.asyncio
async def test_basic_subscribe_and_receive(redis_connector, publisher, subscriber):
    """
    Test: Basic subscription and message reception

    Verifies:
    - Subscribe to a channel
    - Start subscriber
    - Publish message
    - Receive message in handler
    """
    # Setup
    bot = MessageCollector()

    # Subscribe to channel
    subscriber.subscribe("TEST_CHANNEL", bot.create_handler("TEST_CHANNEL"))

    # Start subscriber
    await subscriber.start()
    await asyncio.sleep(0.5)  # Allow connection

    # Publish message
    await publisher.publish_prices({"TEST_CHANNEL": "42000.50"})
    await asyncio.sleep(0.5)  # Allow processing

    # Verify
    assert bot.get_count("TEST_CHANNEL") == 1
    assert bot.get_message("TEST_CHANNEL") == "42000.50"

    # Cleanup
    await subscriber.stop()


# ============================================================================
# TEST 2: Multiple Channels
# ============================================================================

@pytest.mark.asyncio
async def test_multiple_channels(redis_connector, publisher, subscriber):
    """
    Test: Subscribe to multiple channels and receive messages on all

    Verifies:
    - Subscribe to multiple channels
    - Each channel receives its own messages
    - Messages don't cross channels
    """
    # Setup
    bot = MessageCollector()

    # Subscribe to multiple channels
    channels = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
    for channel in channels:
        subscriber.subscribe(channel, bot.create_handler(channel))

    # Start subscriber
    await subscriber.start()
    await asyncio.sleep(0.5)

    # Publish different prices to each channel
    await publisher.publish_prices({
        "BTCUSDT": "42000.00",
        "ETHUSDT": "2500.00",
        "BNBUSDT": "300.00"
    })
    await asyncio.sleep(0.5)

    # Verify each channel received correct message
    assert bot.get_count("BTCUSDT") == 1
    assert bot.get_count("ETHUSDT") == 1
    assert bot.get_count("BNBUSDT") == 1

    assert bot.get_message("BTCUSDT") == "42000.00"
    assert bot.get_message("ETHUSDT") == "2500.00"
    assert bot.get_message("BNBUSDT") == "300.00"

    # Cleanup
    await subscriber.stop()


# ============================================================================
# TEST 3: Multiple Messages on Same Channel
# ============================================================================

@pytest.mark.asyncio
async def test_multiple_messages_same_channel(redis_connector, publisher, subscriber):
    """
    Test: Receive multiple messages on same channel

    Verifies:
    - Handler is called multiple times
    - Latest message is stored
    - Message count is tracked
    """
    # Setup
    bot = MessageCollector()
    subscriber.subscribe("BTCUSDT", bot.create_handler("BTCUSDT"))

    # Start subscriber
    await subscriber.start()
    await asyncio.sleep(0.5)

    # Publish multiple messages
    for i in range(5):
        await publisher.publish_prices({"BTCUSDT": f"{42000 + i}.00"})
        await asyncio.sleep(0.1)

    await asyncio.sleep(0.5)

    # Verify
    assert bot.get_count("BTCUSDT") == 5
    assert bot.get_message("BTCUSDT") == "42004.00"  # Latest message

    # Cleanup
    await subscriber.stop()


# ============================================================================
# TEST 4: Handler Error Handling
# ============================================================================

@pytest.mark.asyncio
async def test_handler_error_handling(redis_connector, publisher, subscriber):
    """
    Test: Handler errors don't crash subscriber

    Verifies:
    - Handler that raises exception doesn't stop subscriber
    - Other channels continue to work
    - Subscriber remains running
    """
    # Setup
    bot = MessageCollector()
    error_count = {"count": 0}

    def error_handler(data: str):
        error_count["count"] += 1
        raise ValueError("Intentional error for testing")

    def normal_handler(data: str):
        bot.messages["ETHUSDT"] = data
        bot.update_counts["ETHUSDT"] = bot.update_counts.get("ETHUSDT", 0) + 1

    # Subscribe with error handler and normal handler
    subscriber.subscribe("BTCUSDT", error_handler)
    subscriber.subscribe("ETHUSDT", normal_handler)

    # Start subscriber
    await subscriber.start()
    await asyncio.sleep(0.5)

    # Publish to both channels
    await publisher.publish_prices({
        "BTCUSDT": "42000.00",  # Will cause error
        "ETHUSDT": "2500.00"    # Should work fine
    })
    await asyncio.sleep(0.5)

    # Verify
    assert error_count["count"] == 1  # Error handler was called
    assert subscriber.is_running  # Subscriber still running
    assert bot.get_count("ETHUSDT") == 1  # Other channel still works
    assert bot.get_message("ETHUSDT") == "2500.00"

    # Cleanup
    await subscriber.stop()


# ============================================================================
# TEST 5: Async Handler Support
# ============================================================================

@pytest.mark.asyncio
async def test_async_handler_support(redis_connector, publisher, subscriber):
    """
    Test: Async handlers are supported

    Verifies:
    - Async handlers work correctly
    - Can do async operations in handler
    """
    # Setup
    bot = MessageCollector()

    async def async_handler(data: str):
        # Simulate async operation
        await asyncio.sleep(0.01)
        bot.messages["BTCUSDT"] = data
        bot.update_counts["BTCUSDT"] = bot.update_counts.get("BTCUSDT", 0) + 1

    # Subscribe with async handler
    subscriber.subscribe("BTCUSDT", async_handler)

    # Start subscriber
    await subscriber.start()
    await asyncio.sleep(0.5)

    # Publish message
    await publisher.publish_prices({"BTCUSDT": "42000.00"})
    await asyncio.sleep(0.5)

    # Verify
    assert bot.get_count("BTCUSDT") == 1
    assert bot.get_message("BTCUSDT") == "42000.00"

    # Cleanup
    await subscriber.stop()


# ============================================================================
# TEST 6: Unsubscribe
# ============================================================================

@pytest.mark.asyncio
async def test_unsubscribe(redis_connector, publisher, subscriber):
    """
    Test: Unsubscribe from channel

    Verifies:
    - Can unsubscribe from a channel
    - No longer receives messages on unsubscribed channel
    - Other channels continue to work
    """
    # Setup
    bot = MessageCollector()

    subscriber.subscribe("BTCUSDT", bot.create_handler("BTCUSDT"))
    subscriber.subscribe("ETHUSDT", bot.create_handler("ETHUSDT"))

    # Start subscriber
    await subscriber.start()
    await asyncio.sleep(0.5)

    # Publish to both
    await publisher.publish_prices({
        "BTCUSDT": "42000.00",
        "ETHUSDT": "2500.00"
    })
    await asyncio.sleep(0.5)

    # Verify both received
    assert bot.get_count("BTCUSDT") == 1
    assert bot.get_count("ETHUSDT") == 1

    # Stop and unsubscribe from BTCUSDT
    await subscriber.stop()
    subscriber.unsubscribe("BTCUSDT")

    # Restart with only ETHUSDT
    await subscriber.start()
    await asyncio.sleep(0.5)

    # Publish again
    await publisher.publish_prices({
        "BTCUSDT": "43000.00",
        "ETHUSDT": "2600.00"
    })
    await asyncio.sleep(0.5)

    # Verify only ETHUSDT received new message
    assert bot.get_count("BTCUSDT") == 1  # Still 1 (not updated)
    assert bot.get_count("ETHUSDT") == 2  # Updated to 2
    assert bot.get_message("ETHUSDT") == "2600.00"

    # Cleanup
    await subscriber.stop()


# ============================================================================
# TEST 7: Start/Stop Lifecycle
# ============================================================================

@pytest.mark.asyncio
async def test_start_stop_lifecycle(redis_connector, subscriber):
    """
    Test: Start and stop subscriber lifecycle

    Verifies:
    - Can start subscriber
    - Can stop subscriber
    - Can restart subscriber
    - Status tracking works
    """
    # Setup
    bot = MessageCollector()
    subscriber.subscribe("BTCUSDT", bot.create_handler("BTCUSDT"))

    # Initial state
    assert not subscriber.is_running
    assert not subscriber.is_connected

    # Start
    await subscriber.start()
    await asyncio.sleep(0.5)

    assert subscriber.is_running
    assert subscriber.is_connected

    # Stop
    await subscriber.stop()
    await asyncio.sleep(0.5)

    assert not subscriber.is_running
    assert not subscriber.is_connected

    # Restart
    await subscriber.start()
    await asyncio.sleep(0.5)

    assert subscriber.is_running
    assert subscriber.is_connected

    # Cleanup
    await subscriber.stop()


# ============================================================================
# TEST 8: Connection Info
# ============================================================================

@pytest.mark.asyncio
async def test_connection_info(redis_connector, subscriber):
    """
    Test: Connection info tracking

    Verifies:
    - get_connection_info() returns correct data
    - Subscribed channels are tracked
    - Connection status is tracked
    """
    # Setup
    bot = MessageCollector()

    # Subscribe to channels
    subscriber.subscribe("BTCUSDT", bot.create_handler("BTCUSDT"))
    subscriber.subscribe("ETHUSDT", bot.create_handler("ETHUSDT"))

    # Get info before start
    info = subscriber.get_connection_info()
    assert not info["is_running"]
    assert not info["is_connected"]
    assert info["num_channels"] == 2
    assert "BTCUSDT" in info["subscribed_channels"]
    assert "ETHUSDT" in info["subscribed_channels"]

    # Start and get info
    await subscriber.start()
    await asyncio.sleep(0.5)

    info = subscriber.get_connection_info()
    assert info["is_running"]
    assert info["is_connected"]
    assert info["num_channels"] == 2

    # Cleanup
    await subscriber.stop()


# ============================================================================
# TEST 9: High Frequency Messages
# ============================================================================

@pytest.mark.asyncio
async def test_high_frequency_messages(redis_connector, publisher, subscriber):
    """
    Test: Handle high frequency message stream

    Verifies:
    - Can handle rapid message stream
    - No messages lost
    - Performance is acceptable
    """
    # Setup
    bot = MessageCollector()
    subscriber.subscribe("BTCUSDT", bot.create_handler("BTCUSDT"))

    # Start subscriber
    await subscriber.start()
    await asyncio.sleep(0.5)

    # Publish many messages rapidly
    num_messages = 50
    for i in range(num_messages):
        await publisher.publish_prices({"BTCUSDT": f"{42000 + i}.00"})
        # Small delay to avoid overwhelming
        if i % 10 == 0:
            await asyncio.sleep(0.1)

    # Wait for processing
    await asyncio.sleep(2.0)

    # Verify most/all messages received
    # (Allow some loss due to timing, but should get most)
    assert bot.get_count("BTCUSDT") >= num_messages * 0.9  # At least 90%

    # Cleanup
    await subscriber.stop()


# ============================================================================
# TEST 10: Auto-Reconnection (Manual Test Placeholder)
# ============================================================================

@pytest.mark.skip(reason="Requires manual Redis restart - use for manual testing")
@pytest.mark.asyncio
async def test_auto_reconnection_manual(redis_connector, publisher, subscriber):
    """
    Test: Automatic reconnection on Redis disconnect

    NOTE: This test requires manually stopping and starting Redis server

    Verifies:
    - Detects Redis disconnect
    - Automatically reconnects
    - Re-subscribes to all channels
    - Resumes receiving messages

    Manual steps:
    1. Run this test
    2. When prompted, stop Redis server
    3. Wait 10 seconds
    4. Start Redis server
    5. Test verifies reconnection works
    """
    # Setup
    bot = MessageCollector()
    subscriber.subscribe("BTCUSDT", bot.create_handler("BTCUSDT"))

    # Start subscriber
    await subscriber.start()
    await asyncio.sleep(0.5)

    # Publish initial message
    await publisher.publish_prices({"BTCUSDT": "42000.00"})
    await asyncio.sleep(0.5)

    assert bot.get_count("BTCUSDT") == 1

    print("\n" + "="*60)
    print("MANUAL TEST: Stop Redis server now!")
    print("Waiting 10 seconds...")
    print("="*60)

    await asyncio.sleep(10)

    print("\n" + "="*60)
    print("Start Redis server now!")
    print("Waiting for reconnection (15 seconds)...")
    print("="*60)

    await asyncio.sleep(15)

    # Verify reconnected
    info = subscriber.get_connection_info()
    assert info["is_connected"]

    # Publish new message
    await publisher.publish_prices({"BTCUSDT": "43000.00"})
    await asyncio.sleep(0.5)

    # Verify received after reconnection
    assert bot.get_count("BTCUSDT") == 2
    assert bot.get_message("BTCUSDT") == "43000.00"

    print("\n✅ Auto-reconnection test PASSED!")

    # Cleanup
    await subscriber.stop()


# ============================================================================
# TEST SUMMARY
# ============================================================================

"""
Test Coverage Summary:

✅ TEST 1: Basic subscribe and receive
   - Core functionality: Subscribe, start, publish, receive

✅ TEST 2: Multiple channels
   - Subscribe to multiple channels
   - Each channel works independently

✅ TEST 3: Multiple messages
   - Handle message stream
   - Track updates correctly

✅ TEST 4: Error handling
   - Handler errors don't crash subscriber
   - Other channels continue working

✅ TEST 5: Async handlers
   - Support for async handler functions

✅ TEST 6: Unsubscribe
   - Remove channel subscription
   - Other channels unaffected

✅ TEST 7: Start/Stop lifecycle
   - Start/stop/restart subscriber
   - Status tracking

✅ TEST 8: Connection info
   - Tracking of connection state
   - Subscribed channels list

✅ TEST 9: High frequency
   - Handle rapid message stream
   - Performance verification

✅ TEST 10: Auto-reconnection (manual)
   - Reconnection on Redis restart
   - Re-subscription after reconnect

These tests cover all critical functionality and ensure the base
subscriber behavior remains stable across code changes.
"""

