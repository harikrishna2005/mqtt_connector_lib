"""
Complete Publisher-Subscriber Test

Tests the full flow: Publisher sends prices → Subscriber receives and updates state
"""

import asyncio
import random
import logging
from mqtt_connector_lib.redis_connector import (
    RedisConnector,
    HighSpeedPublisher,
    HighSpeedSubscriber
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class SimpleTradingBot:
    """Simple bot that tracks prices"""

    def __init__(self):
        self.prices = {}
        self.update_counts = {}

    def create_handler(self, symbol: str):
        """Factory method to create a handler for a specific symbol"""
        def handler(price_str: str):
            try:
                price = float(price_str)
                self.prices[symbol] = price
                self.update_counts[symbol] = self.update_counts.get(symbol, 0) + 1
            except ValueError:
                logger.error(f"Invalid price for {symbol}: {price_str}")
        return handler

    def get_status(self) -> str:
        """Get current status as string"""
        lines = []
        for symbol in sorted(self.prices.keys()):
            price = self.prices[symbol]
            count = self.update_counts[symbol]
            lines.append(f"  {symbol}: ${price:,.2f} ({count} updates)")
        return "\n".join(lines) if lines else "  No updates yet"


async def publisher_task(publisher: HighSpeedPublisher, duration: int = 30):
    """
    Simulate live price publisher

    Publishes price updates every second for multiple symbols
    """
    print("📤 Publisher started")

    # Base prices
    base_prices = {
        "BTCUSDT": 42000.0,
        "ETHUSDT": 2500.0,
        "BNBUSDT": 300.0,
        "XRPUSDT": 0.50,
    }

    for second in range(duration):
        # Generate prices with random variation
        prices = {}
        for symbol, base_price in base_prices.items():
            variation = random.uniform(-0.005, 0.005)  # ±0.5%
            price = base_price * (1 + variation)
            prices[symbol] = round(price, 8)

        # Publish
        success = await publisher.publish_prices(prices)

        if second % 10 == 0:
            status = "✅" if success else "❌"
            print(f"📤 Published batch {second // 10 + 1} {status}")

        await asyncio.sleep(1)

    print("📤 Publisher finished")


async def subscriber_task(subscriber: HighSpeedSubscriber, bot: SimpleTradingBot, duration: int = 30):
    """
    Monitor subscriber and print updates
    """
    print("📥 Subscriber started")

    await subscriber.start()

    # Print status periodically
    for second in range(0, duration, 10):
        await asyncio.sleep(10)
        print(f"\n📊 Status after {second + 10} seconds:")
        print(bot.get_status())

    await subscriber.stop()
    print("📥 Subscriber finished")


async def test_pubsub():
    """
    Complete publisher-subscriber test
    """

    print("\n" + "=" * 70)
    print("🔄 REDIS PUB/SUB TEST")
    print("=" * 70)
    print("Testing: Publisher → Redis → Subscriber → Bot State Update")
    print("=" * 70)
    print()

    # Setup
    connector = RedisConnector(
        host="192.168.29.42",
        port=6379,
        password="SuperDuperRedis6748@"
    )

    publisher = HighSpeedPublisher(connector, max_concurrent_tasks=10)

    # Create bot and subscriber
    bot = SimpleTradingBot()
    subscriber = HighSpeedSubscriber(connector)

    # Subscribe to channels
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT"]
    for symbol in symbols:
        handler = bot.create_handler(symbol)
        subscriber.subscribe(symbol, handler)

    print(f"✅ Setup complete")
    print(f"   Subscribed to: {', '.join(symbols)}")
    print()

    # Run publisher and subscriber concurrently
    duration = 30  # 30 seconds test

    try:
        await asyncio.gather(
            publisher_task(publisher, duration),
            subscriber_task(subscriber, bot, duration)
        )
    except KeyboardInterrupt:
        print("\n⚠️ Test interrupted")
    finally:
        await subscriber.stop()
        await connector.close()

    # Final results
    print("\n" + "=" * 70)
    print("📊 FINAL RESULTS")
    print("=" * 70)
    print(bot.get_status())
    print("=" * 70)

    # Verify
    total_updates = sum(bot.update_counts.values())
    if total_updates > 0:
        print(f"\n✅ SUCCESS: Received {total_updates} total updates")
    else:
        print("\n❌ FAILED: No updates received")

    print()


if __name__ == "__main__":
    asyncio.run(test_pubsub())

