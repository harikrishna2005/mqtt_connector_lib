"""
Trading Bot Example with Redis Subscriber

Demonstrates how to use HighSpeedSubscriber in a real trading bot
to receive live price updates and update internal state.
"""

import asyncio
import logging
from mqtt_connector_lib.redis_connector import RedisConnector, HighSpeedSubscriber

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class TradingBot:
    """
    Example trading bot that subscribes to live price updates.
    """

    def __init__(self):
        # Live prices (updated by subscriber)
        self.btc_price = 0.0
        self.eth_price = 0.0
        self.bnb_price = 0.0
        self.xrp_price = 0.0

        # Price update counters
        self.btc_updates = 0
        self.eth_updates = 0
        self.bnb_updates = 0
        self.xrp_updates = 0

        # Trading logic state
        self.last_btc_signal = None
        self.last_eth_signal = None

    # Handler methods for price updates

    def update_btc_price(self, price_str: str):
        """Handler for BTCUSDT price updates"""
        try:
            self.btc_price = float(price_str)
            self.btc_updates += 1

            # Example: Simple trading logic
            if self.btc_price > 45000:
                if self.last_btc_signal != "SELL":
                    logger.info(f"💰 BTC Signal: SELL at {self.btc_price}")
                    self.last_btc_signal = "SELL"
            elif self.btc_price < 40000:
                if self.last_btc_signal != "BUY":
                    logger.info(f"💰 BTC Signal: BUY at {self.btc_price}")
                    self.last_btc_signal = "BUY"
        except ValueError as e:
            logger.error(f"❌ Invalid BTC price: {price_str}")

    def update_eth_price(self, price_str: str):
        """Handler for ETHUSDT price updates"""
        try:
            self.eth_price = float(price_str)
            self.eth_updates += 1

            # Example: Simple trading logic
            if self.eth_price > 2600:
                if self.last_eth_signal != "SELL":
                    logger.info(f"💰 ETH Signal: SELL at {self.eth_price}")
                    self.last_eth_signal = "SELL"
            elif self.eth_price < 2400:
                if self.last_eth_signal != "BUY":
                    logger.info(f"💰 ETH Signal: BUY at {self.eth_price}")
                    self.last_eth_signal = "BUY"
        except ValueError as e:
            logger.error(f"❌ Invalid ETH price: {price_str}")

    def update_bnb_price(self, price_str: str):
        """Handler for BNBUSDT price updates"""
        try:
            self.bnb_price = float(price_str)
            self.bnb_updates += 1
        except ValueError as e:
            logger.error(f"❌ Invalid BNB price: {price_str}")

    def update_xrp_price(self, price_str: str):
        """Handler for XRPUSDT price updates"""
        try:
            self.xrp_price = float(price_str)
            self.xrp_updates += 1
        except ValueError as e:
            logger.error(f"❌ Invalid XRP price: {price_str}")

    def print_portfolio_status(self):
        """Print current portfolio status"""
        print("\n" + "=" * 60)
        print("📊 TRADING BOT - LIVE PRICES")
        print("=" * 60)
        print(f"BTC: ${self.btc_price:,.2f} (Updates: {self.btc_updates})")
        print(f"ETH: ${self.eth_price:,.2f} (Updates: {self.eth_updates})")
        print(f"BNB: ${self.bnb_price:,.2f} (Updates: {self.bnb_updates})")
        print(f"XRP: ${self.xrp_price:,.2f} (Updates: {self.xrp_updates})")
        print("=" * 60)
        print()


async def main():
    """
    Main example - Real trading bot usage pattern
    """

    print("\n" + "=" * 60)
    print("🤖 TRADING BOT WITH REDIS SUBSCRIBER")
    print("=" * 60)
    print("This example shows how to use HighSpeedSubscriber")
    print("to receive live price updates in your trading bot.")
    print("=" * 60)
    print()

    # 1. Create Redis connector
    connector = RedisConnector(
        host="192.168.29.42",
        port=6379,
        password="SuperDuperRedis6748@"
    )

    # 2. Create trading bot instance
    bot = TradingBot()

    # 3. Create subscriber
    subscriber = HighSpeedSubscriber(connector)

    # 4. Subscribe to channels with handler methods
    print("📝 Subscribing to price channels...")
    subscriber.subscribe("BTCUSDT", bot.update_btc_price)
    subscriber.subscribe("ETHUSDT", bot.update_eth_price)
    subscriber.subscribe("BNBUSDT", bot.update_bnb_price)
    subscriber.subscribe("XRPUSDT", bot.update_xrp_price)
    print()

    # 5. Start subscriber
    await subscriber.start()

    try:
        # 6. Run monitoring loop
        print("🎧 Listening for price updates... (Press Ctrl+C to stop)")
        print()

        # Print status every 10 seconds
        for i in range(30):  # Run for 5 minutes
            await asyncio.sleep(10)
            bot.print_portfolio_status()

            # Check if we're getting updates
            total_updates = (bot.btc_updates + bot.eth_updates +
                           bot.bnb_updates + bot.xrp_updates)
            if total_updates == 0 and i > 0:
                print("⚠️ No price updates received yet. Is the publisher running?")

    except KeyboardInterrupt:
        print("\n⚠️ Shutting down trading bot...")

    finally:
        # 7. Cleanup
        print("\n🧹 Cleaning up...")
        await subscriber.stop()
        await connector.close()

        # Final status
        print("\n" + "=" * 60)
        print("📊 FINAL STATISTICS")
        print("=" * 60)
        print(f"Total BTC updates: {bot.btc_updates}")
        print(f"Total ETH updates: {bot.eth_updates}")
        print(f"Total BNB updates: {bot.bnb_updates}")
        print(f"Total XRP updates: {bot.xrp_updates}")
        print(f"Total updates:     {bot.btc_updates + bot.eth_updates + bot.bnb_updates + bot.xrp_updates}")
        print("=" * 60)
        print("\n✅ Trading bot stopped")


async def simple_example():
    """
    Simple minimal example
    """

    print("\n🔵 SIMPLE SUBSCRIBER EXAMPLE\n")

    # Simple handler functions
    def handle_btc(price):
        print(f"📈 BTC: ${float(price):,.2f}")

    def handle_eth(price):
        print(f"📈 ETH: ${float(price):,.2f}")

    # Setup
    connector = RedisConnector(
        host="192.168.29.42",
        port=6379,
        password="SuperDuperRedis6748@"
    )

    subscriber = HighSpeedSubscriber(connector)
    subscriber.subscribe("BTCUSDT", handle_btc)
    subscriber.subscribe("ETHUSDT", handle_eth)

    # Start and listen
    await subscriber.start()

    try:
        print("🎧 Listening... (Ctrl+C to stop)\n")
        await asyncio.sleep(60)  # Listen for 1 minute
    except KeyboardInterrupt:
        pass
    finally:
        await subscriber.stop()
        await connector.close()
        print("\n✅ Done")


if __name__ == "__main__":
    # Run the full trading bot example
    asyncio.run(main())

    # Or run the simple example:
    # asyncio.run(simple_example())

