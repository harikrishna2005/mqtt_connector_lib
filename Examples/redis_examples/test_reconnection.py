"""
Redis Reconnection Test

Demonstrates automatic reconnection and re-subscription when Redis server
restarts or connection is lost.

Test scenario:
1. Start subscriber with handlers
2. Receive some messages
3. Simulate Redis disconnect (stop Redis server or kill connection)
4. Subscriber automatically detects disconnect
5. Subscriber waits 5 seconds
6. Subscriber reconnects to Redis
7. Subscriber re-subscribes to all channels
8. Messages continue to be received

Handler registry is preserved in memory, so no need to store in Redis.
"""

import asyncio
import logging
from mqtt_connector_lib.redis_connector import (
    RedisConnector,
    HighSpeedPublisher,
    HighSpeedSubscriber
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class TradingBot:
    """Simple bot to test reconnection"""

    def __init__(self):
        self.prices = {}
        self.update_counts = {}
        self.last_connection_status = None

    def update_btc_price(self, price_str: str):
        try:
            price = float(price_str)
            self.prices['BTCUSDT'] = price
            self.update_counts['BTCUSDT'] = self.update_counts.get('BTCUSDT', 0) + 1
            logger.info(f"💰 BTC updated: ${price:,.2f}")
        except ValueError as e:
            logger.error(f"Invalid BTC price: {price_str}")

    def update_eth_price(self, price_str: str):
        try:
            price = float(price_str)
            self.prices['ETHUSDT'] = price
            self.update_counts['ETHUSDT'] = self.update_counts.get('ETHUSDT', 0) + 1
            logger.info(f"💰 ETH updated: ${price:,.2f}")
        except ValueError as e:
            logger.error(f"Invalid ETH price: {price_str}")


async def test_reconnection():
    """
    Test automatic reconnection

    Instructions:
    1. Run this script
    2. Let it receive some messages
    3. Stop Redis server (or kill the connection)
    4. Wait 5 seconds - subscriber will detect disconnect
    5. Start Redis server again
    6. Subscriber automatically reconnects and re-subscribes
    7. Messages continue to be received
    """

    print("\n" + "=" * 80)
    print("🔄 REDIS RECONNECTION TEST")
    print("=" * 80)
    print("This test demonstrates automatic reconnection and re-subscription.")
    print()
    print("Instructions:")
    print("  1. Script will start and subscribe to channels")
    print("  2. You'll see messages being received")
    print("  3. Stop Redis server to simulate disconnect")
    print("  4. Subscriber will detect disconnect and wait 5 seconds")
    print("  5. Start Redis server again")
    print("  6. Subscriber automatically reconnects and re-subscribes")
    print("  7. Messages continue to flow")
    print("=" * 80)
    print()

    # Setup
    connector = RedisConnector(
        host="192.168.29.42",
        port=6379,
        password="SuperDuperRedis6748@"
    )

    bot = TradingBot()

    # Create subscriber with auto-reconnect enabled
    subscriber = HighSpeedSubscriber(
        connector,
        auto_reconnect=True,           # Enable auto-reconnect
        reconnect_delay=5.0,            # Wait 5 seconds before reconnecting
        max_reconnect_attempts=0        # Infinite attempts (0 = infinite)
    )

    # Subscribe to channels
    subscriber.subscribe("BTCUSDT", bot.update_btc_price)
    subscriber.subscribe("ETHUSDT", bot.update_eth_price)

    # Start subscriber
    await subscriber.start()

    print("✅ Subscriber started with auto-reconnect enabled")
    print(f"   Subscribed to: {subscriber.get_subscribed_channels()}")
    print()
    print("🎧 Listening for messages...")
    print("   (Stop Redis server to test reconnection)")
    print()

    try:
        # Monitor connection status
        last_connected = True

        while True:
            await asyncio.sleep(2)

            # Check connection status
            info = subscriber.get_connection_info()
            is_connected = info['is_connected']

            # Detect status change
            if is_connected != last_connected:
                if is_connected:
                    print("\n" + "=" * 80)
                    print("✅ RECONNECTED!")
                    print(f"   Re-subscribed to: {info['subscribed_channels']}")
                    print("   Messages will continue to flow...")
                    print("=" * 80)
                    print()
                else:
                    print("\n" + "=" * 80)
                    print("❌ DISCONNECTED!")
                    print("   Waiting 5 seconds to reconnect...")
                    print("   (Start Redis server now)")
                    print("=" * 80)
                    print()

                last_connected = is_connected

            # Print status every 10 seconds
            if bot.update_counts:
                total_updates = sum(bot.update_counts.values())
                if total_updates > 0 and total_updates % 5 == 0:
                    print(f"📊 Total updates: {total_updates} | "
                          f"BTC: {bot.update_counts.get('BTCUSDT', 0)} | "
                          f"ETH: {bot.update_counts.get('ETHUSDT', 0)}")

    except KeyboardInterrupt:
        print("\n\n⚠️ Stopping test...")

    finally:
        await subscriber.stop()
        await connector.close()

        print("\n" + "=" * 80)
        print("📊 FINAL STATISTICS")
        print("=" * 80)
        print(f"Total BTC updates: {bot.update_counts.get('BTCUSDT', 0)}")
        print(f"Total ETH updates: {bot.update_counts.get('ETHUSDT', 0)}")
        print(f"Reconnection attempts: {info['reconnect_attempts']}")
        print("=" * 80)
        print("\n✅ Test complete")


async def test_with_publisher():
    """
    Complete test with both publisher and subscriber to show reconnection
    """

    print("\n" + "=" * 80)
    print("🔄 COMPLETE RECONNECTION TEST (with Publisher)")
    print("=" * 80)
    print()

    # Setup connector
    connector = RedisConnector(
        host="192.168.29.42",
        port=6379,
        password="SuperDuperRedis6748@"
    )

    # Setup publisher
    publisher = HighSpeedPublisher(connector)

    # Setup bot and subscriber
    bot = TradingBot()
    subscriber = HighSpeedSubscriber(
        connector,
        auto_reconnect=True,
        reconnect_delay=5.0
    )

    subscriber.subscribe("BTCUSDT", bot.update_btc_price)
    subscriber.subscribe("ETHUSDT", bot.update_eth_price)

    # Start subscriber
    await subscriber.start()

    print("✅ System started")
    print("   Publisher: Ready")
    print("   Subscriber: Listening with auto-reconnect")
    print()
    print("📤 Publishing prices every 2 seconds...")
    print("   (Stop Redis to test reconnection)")
    print()

    try:
        counter = 0
        last_connected = True

        while True:
            # Publish prices
            prices = {
                "BTCUSDT": 42000.0 + counter * 10,
                "ETHUSDT": 2500.0 + counter * 5
            }

            success = await publisher.publish_prices(prices)

            if success:
                counter += 1

            # Check subscriber status
            info = subscriber.get_connection_info()
            is_connected = info['is_connected']

            if is_connected != last_connected:
                if is_connected:
                    print("\n✅ SUBSCRIBER RECONNECTED and RE-SUBSCRIBED!")
                    print("   Continuing to receive messages...\n")
                else:
                    print("\n❌ SUBSCRIBER DISCONNECTED!")
                    print("   Will reconnect in 5 seconds...\n")

                last_connected = is_connected

            await asyncio.sleep(2)

    except KeyboardInterrupt:
        print("\n⚠️ Stopping...")

    finally:
        await subscriber.stop()
        await connector.close()
        print("✅ Test complete")


if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("Choose test:")
    print("  1. Subscriber only (manual test - you stop/start Redis)")
    print("  2. Publisher + Subscriber (complete test)")
    print("=" * 80)

    choice = input("\nEnter choice (1 or 2): ").strip()

    if choice == "2":
        asyncio.run(test_with_publisher())
    else:
        asyncio.run(test_reconnection())

