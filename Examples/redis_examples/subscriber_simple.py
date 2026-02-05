import asyncio
from mqtt_connector_lib.redis_connector import RedisConnector


async def run_subscriber():
    # 1. Setup connection (matching your Raspberry Pi IP)
    connector = RedisConnector(
        host="192.168.29.42",
        port=6379,
        password="SuperDuperRedis6748@"
    )

    # 2. Setup Regular PubSub
    # Note: Sharded pub/sub (ssubscribe) is not available in redis-py async yet
    pubsub = connector.redis_client.pubsub()

    # Subscribe to channels matching the publisher
    # Publisher uses: f"BIN_{pair}" where pair is PAIR_0, PAIR_1, etc.
    channels = [f"BIN_PAIR_{i}" for i in range(50)]  # For stress_test.py
    # channels = ["BIN_AAAAAA", "BIN_BBBBBB", "BIN_CCCCCC"]  # For simple_publisher.py test
    # channels = ["BIN_PAIR_0", "BIN_PAIR_1"]  # Test with fewer channels

    # Use regular subscribe (not ssubscribe)
    print(f"📡 Subscribing to {len(channels)} channels...")
    await pubsub.subscribe(*channels)
    print(f"✅ Subscribed successfully!")

    # 3. Define the listening task
    async def listen():
        message_count = 0
        subscribe_confirmed = 0
        try:
            async for message in pubsub.listen():
                msg_type = message.get('type', '')

                # Debug: print subscription confirmations
                if msg_type == 'subscribe':
                    subscribe_confirmed += 1
                    if subscribe_confirmed == 1 or subscribe_confirmed % 10 == 0:
                        print(f"✅ Subscribed to {subscribe_confirmed} channels...")

                # Handle regular messages
                if msg_type == 'message':
                    channel = message['channel']
                    price = message['data']
                    message_count += 1
                    # Print first message and every 100th message
                    if message_count  <= 20 or message_count % 100 == 0:
                        print(f"📊 Received {message_count} messages. Latest: [{channel}] = {price}")
        except Exception as e:
            print(f"❌ Error: {e}")
        finally:
            print(f"\n📈 Total messages received: {message_count}")

    # 4. Run the listener but set a 30-second timeout
    try:
        # wait_for will cancel the listen task after 30 seconds
        await asyncio.wait_for(listen(), timeout=30.0)
    except asyncio.TimeoutError:
        print("\n⏱ 30 seconds reached. Stopping subscriber.")
    finally:
        # 5. Cleanup
        await pubsub.unsubscribe(*channels)
        await connector.close()


if __name__ == "__main__":
    asyncio.run(run_subscriber())