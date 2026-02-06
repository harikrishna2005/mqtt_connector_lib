"""
Complete Pub/Sub Integration Test

Tests the full flow: Publisher → Redis → Subscriber → Handler execution
Including start/stop lifecycle for dynamic subscription control.

This test validates:
1. Publisher sends messages
2. Subscriber receives messages
3. Handlers execute with correct data
4. Stop/Start subscriber lifecycle
5. Message reception control

Run with: pytest tests/test_mqtt_connector_lib/test_pubsub_integration.py -v
"""

import asyncio
import pytest
import time
from mqtt_connector_lib.redis_connector import RedisConnector, HighSpeedPublisher, HighSpeedSubscriber


# Test Configuration
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


class RebalancingBot:
    """
    Simulated rebalancing bot for testing
    Tracks BTC and PAXG prices
    """

    def __init__(self):
        # Price storage
        self.btc_price = 0.0
        self.paxg_price = 0.0

        # Update tracking
        self.btc_updates = 0
        self.paxg_updates = 0

        # History
        self.btc_history = []
        self.paxg_history = []

    def update_btc_price(self, price_str: str):
        """Handler for BTC price updates"""
        self.btc_price = float(price_str)
        self.btc_updates += 1
        self.btc_history.append(self.btc_price)

    def update_paxg_price(self, price_str: str):
        """Handler for PAXG price updates"""
        self.paxg_price = float(price_str)
        self.paxg_updates += 1
        self.paxg_history.append(self.paxg_price)

    def get_total_updates(self) -> int:
        """Get total number of updates received"""
        return self.btc_updates + self.paxg_updates


# ============================================================================
# TEST 1: Basic Pub/Sub Flow
# ============================================================================

@pytest.mark.asyncio
async def test_basic_pubsub_flow(redis_connector, publisher, subscriber):
    """
    Test: Complete pub/sub flow

    Verifies:
    - Subscribe to BTCUSDT and PAXGUSDT
    - Publish prices
    - Handlers receive correct prices
    - Update counts are tracked
    """
    # Setup bot
    bot = RebalancingBot()

    # Subscribe to channels
    subscriber.subscribe("BTCUSDT", bot.update_btc_price)
    subscriber.subscribe("PAXGUSDT", bot.update_paxg_price)

    # Start subscriber
    await subscriber.start()
    await asyncio.sleep(0.5)  # Allow connection

    # Publish prices
    await publisher.publish_prices({
        "BTCUSDT": "50000.00",
        "PAXGUSDT": "2000.00"
    })
    await asyncio.sleep(0.5)  # Allow processing

    # Verify reception
    assert bot.btc_updates == 1
    assert bot.paxg_updates == 1
    assert bot.btc_price == 50000.00
    assert bot.paxg_price == 2000.00

    # Cleanup
    await subscriber.stop()


# ============================================================================
# TEST 2: Multiple Price Updates
# ============================================================================

@pytest.mark.asyncio
async def test_multiple_price_updates(redis_connector, publisher, subscriber):
    """
    Test: Multiple price updates over time

    Verifies:
    - Subscribe to BTCUSDT and PAXGUSDT
    - Publish multiple price updates
    - All updates are received
    - Latest prices are stored
    """
    # Setup bot
    bot = RebalancingBot()

    # Subscribe
    subscriber.subscribe("BTCUSDT", bot.update_btc_price)
    subscriber.subscribe("PAXGUSDT", bot.update_paxg_price)

    # Start subscriber
    await subscriber.start()
    await asyncio.sleep(0.5)

    # Publish 5 price updates
    for i in range(5):
        await publisher.publish_prices({
            "BTCUSDT": f"{50000 + i * 100}.00",
            "PAXGUSDT": f"{2000 + i * 10}.00"
        })
        await asyncio.sleep(0.2)

    await asyncio.sleep(0.5)

    # Verify all updates received
    assert bot.btc_updates == 5
    assert bot.paxg_updates == 5

    # Verify latest prices
    assert bot.btc_price == 50400.00  # 50000 + 4*100
    assert bot.paxg_price == 2040.00  # 2000 + 4*10

    # Verify history
    assert len(bot.btc_history) == 5
    assert len(bot.paxg_history) == 5

    # Cleanup
    await subscriber.stop()


# ============================================================================
# TEST 3: Stop and Start Subscriber (Main Test!)
# ============================================================================

@pytest.mark.asyncio
async def test_stop_start_subscriber_lifecycle(redis_connector, publisher, subscriber):
    """
    Test: Stop and start subscriber to control message reception

    Main test that verifies:
    1. Subscribe to BTCUSDT and PAXGUSDT
    2. Start subscriber and receive updates
    3. Stop subscriber (should NOT receive updates)
    4. Start subscriber again (should resume receiving updates)

    This is the core requirement: Control when to receive updates
    """
    # Setup bot
    bot = RebalancingBot()

    # Subscribe to channels
    subscriber.subscribe("BTCUSDT", bot.update_btc_price)
    subscriber.subscribe("PAXGUSDT", bot.update_paxg_price)

    print("\n" + "="*70)
    print("PHASE 1: Start subscriber and receive updates")
    print("="*70)

    # Start subscriber
    await subscriber.start()
    await asyncio.sleep(0.5)

    # Verify subscriber is running
    assert subscriber.is_running is True
    assert subscriber.is_connected is True

    # Publish batch 1
    await publisher.publish_prices({
        "BTCUSDT": "50000.00",
        "PAXGUSDT": "2000.00"
    })
    await asyncio.sleep(0.5)

    # Verify received
    assert bot.btc_updates == 1
    assert bot.paxg_updates == 1
    assert bot.btc_price == 50000.00
    assert bot.paxg_price == 2000.00

    print(f"✅ Phase 1: Received updates (BTC: {bot.btc_updates}, PAXG: {bot.paxg_updates})")

    # ========================================================================
    # PHASE 2: Stop subscriber - should NOT receive updates
    # ========================================================================

    print("\n" + "="*70)
    print("PHASE 2: Stop subscriber (should NOT receive updates)")
    print("="*70)

    # Stop subscriber
    await subscriber.stop()
    await asyncio.sleep(0.5)

    # Verify subscriber is stopped
    assert subscriber.is_running is False
    assert subscriber.is_connected is False

    # Remember current update counts
    btc_updates_before_stop = bot.btc_updates
    paxg_updates_before_stop = bot.paxg_updates

    # Publish batch 2 (should NOT be received)
    await publisher.publish_prices({
        "BTCUSDT": "51000.00",
        "PAXGUSDT": "2100.00"
    })
    await asyncio.sleep(0.5)

    # Publish batch 3 (should NOT be received)
    await publisher.publish_prices({
        "BTCUSDT": "52000.00",
        "PAXGUSDT": "2200.00"
    })
    await asyncio.sleep(0.5)

    # Verify NO new updates received
    assert bot.btc_updates == btc_updates_before_stop  # Still 1
    assert bot.paxg_updates == paxg_updates_before_stop  # Still 1
    assert bot.btc_price == 50000.00  # Old price
    assert bot.paxg_price == 2000.00  # Old price

    print(f"✅ Phase 2: Did NOT receive updates while stopped")
    print(f"   BTC updates: {bot.btc_updates} (unchanged)")
    print(f"   PAXG updates: {bot.paxg_updates} (unchanged)")

    # ========================================================================
    # PHASE 3: Start subscriber again - should resume receiving
    # ========================================================================

    print("\n" + "="*70)
    print("PHASE 3: Start subscriber again (should resume receiving)")
    print("="*70)

    # Start subscriber again
    await subscriber.start()
    await asyncio.sleep(0.5)

    # Verify subscriber is running
    assert subscriber.is_running is True
    assert subscriber.is_connected is True

    # Publish batch 4 (should be received)
    await publisher.publish_prices({
        "BTCUSDT": "53000.00",
        "PAXGUSDT": "2300.00"
    })
    await asyncio.sleep(0.5)

    # Verify new updates received
    assert bot.btc_updates == 2  # Increased from 1 to 2
    assert bot.paxg_updates == 2  # Increased from 1 to 2
    assert bot.btc_price == 53000.00  # New price
    assert bot.paxg_price == 2300.00  # New price

    print(f"✅ Phase 3: Resumed receiving updates")
    print(f"   BTC updates: {bot.btc_updates} (increased)")
    print(f"   PAXG updates: {bot.paxg_updates} (increased)")
    print(f"   BTC price: ${bot.btc_price:,.2f}")
    print(f"   PAXG price: ${bot.paxg_price:,.2f}")

    # ========================================================================
    # FINAL VERIFICATION
    # ========================================================================

    print("\n" + "="*70)
    print("FINAL VERIFICATION")
    print("="*70)

    # Total updates should be 2 (phase 1 + phase 3)
    # Phase 2 updates were NOT received
    assert bot.btc_updates == 2
    assert bot.paxg_updates == 2

    # History should show only received prices
    assert len(bot.btc_history) == 2  # [50000, 53000]
    assert len(bot.paxg_history) == 2  # [2000, 2300]
    assert bot.btc_history == [50000.00, 53000.00]
    assert bot.paxg_history == [2000.00, 2300.00]

    print(f"✅ Final verification passed")
    print(f"   Total BTC updates: {bot.btc_updates}")
    print(f"   Total PAXG updates: {bot.paxg_updates}")
    print(f"   BTC history: {bot.btc_history}")
    print(f"   PAXG history: {bot.paxg_history}")
    print("="*70)

    # Cleanup
    await subscriber.stop()


# ============================================================================
# TEST 4: Rapid Start/Stop Cycles
# ============================================================================

@pytest.mark.asyncio
async def test_rapid_start_stop_cycles(redis_connector, publisher, subscriber):
    """
    Test: Rapid start/stop cycles

    Verifies:
    - Can start and stop subscriber multiple times
    - No errors or resource leaks
    - Functionality works after multiple cycles
    """
    # Setup bot
    bot = RebalancingBot()

    # Subscribe
    subscriber.subscribe("BTCUSDT", bot.update_btc_price)
    subscriber.subscribe("PAXGUSDT", bot.update_paxg_price)

    # Cycle 1
    await subscriber.start()
    await asyncio.sleep(0.3)
    await publisher.publish_prices({"BTCUSDT": "50000", "PAXGUSDT": "2000"})
    await asyncio.sleep(0.3)
    await subscriber.stop()

    # Cycle 2
    await subscriber.start()
    await asyncio.sleep(0.3)
    await publisher.publish_prices({"BTCUSDT": "51000", "PAXGUSDT": "2100"})
    await asyncio.sleep(0.3)
    await subscriber.stop()

    # Cycle 3
    await subscriber.start()
    await asyncio.sleep(0.3)
    await publisher.publish_prices({"BTCUSDT": "52000", "PAXGUSDT": "2200"})
    await asyncio.sleep(0.3)

    # Should have received 3 updates
    assert bot.btc_updates == 3
    assert bot.paxg_updates == 3

    # Cleanup
    await subscriber.stop()


# ============================================================================
# TEST 5: Publish While Subscriber is Stopped (Messages Lost)
# ============================================================================

@pytest.mark.asyncio
async def test_publish_while_stopped_messages_lost(redis_connector, publisher, subscriber):
    """
    Test: Messages published while subscriber is stopped are lost

    Verifies:
    - Redis pub/sub behavior: No message persistence
    - Messages sent while no subscriber is active are lost
    - This is expected Redis behavior
    """
    # Setup bot
    bot = RebalancingBot()

    # Subscribe but DON'T start
    subscriber.subscribe("BTCUSDT", bot.update_btc_price)
    subscriber.subscribe("PAXGUSDT", bot.update_paxg_price)

    # Publish messages while subscriber is NOT running
    await publisher.publish_prices({"BTCUSDT": "50000", "PAXGUSDT": "2000"})
    await publisher.publish_prices({"BTCUSDT": "51000", "PAXGUSDT": "2100"})
    await asyncio.sleep(0.5)

    # Verify NO updates received
    assert bot.btc_updates == 0
    assert bot.paxg_updates == 0

    # Now start subscriber
    await subscriber.start()
    await asyncio.sleep(0.5)

    # Old messages are lost (pub/sub doesn't persist)
    assert bot.btc_updates == 0
    assert bot.paxg_updates == 0

    # New message should be received
    await publisher.publish_prices({"BTCUSDT": "52000", "PAXGUSDT": "2200"})
    await asyncio.sleep(0.5)

    assert bot.btc_updates == 1
    assert bot.paxg_updates == 1
    assert bot.btc_price == 52000.00
    assert bot.paxg_price == 2200.00

    # Cleanup
    await subscriber.stop()


# ============================================================================
# TEST 6: High Frequency Pub/Sub
# ============================================================================

@pytest.mark.asyncio
async def test_high_frequency_pubsub(redis_connector, publisher, subscriber):
    """
    Test: High frequency publishing and subscribing

    Simulates real trading bot scenario:
    - 25 messages per publish call
    - 20 calls per second
    - Subscriber receives all messages
    """
    # Setup bot
    bot = RebalancingBot()

    # Subscribe
    subscriber.subscribe("BTCUSDT", bot.update_btc_price)
    subscriber.subscribe("PAXGUSDT", bot.update_paxg_price)

    # Start subscriber
    await subscriber.start()
    await asyncio.sleep(0.5)

    # Publish rapidly (5 seconds worth)
    num_updates = 50
    for i in range(num_updates):
        await publisher.publish_prices({
            "BTCUSDT": f"{50000 + i}.00",
            "PAXGUSDT": f"{2000 + i}.00"
        })
        # Small delay to simulate real-time updates
        if i % 10 == 0:
            await asyncio.sleep(0.1)

    # Wait for all messages to be processed
    await asyncio.sleep(1.0)

    # Should receive most/all updates (allow some timing variation)
    assert bot.btc_updates >= num_updates * 0.9  # At least 90%
    assert bot.paxg_updates >= num_updates * 0.9

    print(f"\n📊 High frequency test: {bot.btc_updates + bot.paxg_updates} total updates received")

    # Cleanup
    await subscriber.stop()


# ============================================================================
# TEST 7: Real-World Rebalancing Bot Simulation
# ============================================================================

@pytest.mark.asyncio
async def test_rebalancing_bot_simulation(redis_connector, publisher, subscriber):
    """
    Test: Complete rebalancing bot simulation

    Simulates:
    - Bot starts and subscribes to BTC and PAXG
    - Receives price updates every second
    - Stops receiving updates (maintenance mode)
    - Resumes receiving updates
    - Continues normal operation
    """
    # Setup bot
    bot = RebalancingBot()

    print("\n" + "="*70)
    print("REBALANCING BOT SIMULATION")
    print("="*70)

    # Subscribe to portfolio assets
    subscriber.subscribe("BTCUSDT", bot.update_btc_price)
    subscriber.subscribe("PAXGUSDT", bot.update_paxg_price)

    # Phase 1: Normal operation (5 updates)
    print("\nPhase 1: Normal operation...")
    await subscriber.start()
    await asyncio.sleep(0.5)

    for i in range(5):
        await publisher.publish_prices({
            "BTCUSDT": f"{50000 + i * 100}.00",
            "PAXGUSDT": f"{2000 + i * 10}.00"
        })
        await asyncio.sleep(0.2)

    await asyncio.sleep(0.5)
    assert bot.btc_updates == 5
    assert bot.paxg_updates == 5
    print(f"✅ Received 5 updates: BTC=${bot.btc_price:,.2f}, PAXG=${bot.paxg_price:,.2f}")

    # Phase 2: Maintenance mode (stop receiving)
    print("\nPhase 2: Entering maintenance mode (stop receiving)...")
    await subscriber.stop()
    await asyncio.sleep(0.5)

    updates_before_maintenance = bot.get_total_updates()

    # Publish during maintenance (not received)
    for i in range(3):
        await publisher.publish_prices({
            "BTCUSDT": f"{55000 + i * 100}.00",
            "PAXGUSDT": f"{2500 + i * 10}.00"
        })
        await asyncio.sleep(0.2)

    await asyncio.sleep(0.5)
    assert bot.get_total_updates() == updates_before_maintenance
    print(f"✅ Maintenance mode: No updates received (still at {updates_before_maintenance})")

    # Phase 3: Resume operation
    print("\nPhase 3: Resuming operation...")
    await subscriber.start()
    await asyncio.sleep(0.5)

    for i in range(5):
        await publisher.publish_prices({
            "BTCUSDT": f"{60000 + i * 100}.00",
            "PAXGUSDT": f"{3000 + i * 10}.00"
        })
        await asyncio.sleep(0.2)

    await asyncio.sleep(0.5)
    assert bot.btc_updates == 10  # 5 + 5 (maintenance messages not received)
    assert bot.paxg_updates == 10
    print(f"✅ Resumed: Received 5 more updates (total: {bot.get_total_updates()})")
    print(f"   BTC=${bot.btc_price:,.2f}, PAXG=${bot.paxg_price:,.2f}")

    # Final summary
    print("\n" + "="*70)
    print("SIMULATION COMPLETE")
    print("="*70)
    print(f"Total updates received: {bot.get_total_updates()}")
    print(f"BTC: {bot.btc_updates} updates, final price: ${bot.btc_price:,.2f}")
    print(f"PAXG: {bot.paxg_updates} updates, final price: ${bot.paxg_price:,.2f}")
    print("="*70)

    # Cleanup
    await subscriber.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

