import asyncio
from mqtt_connector_lib.redis_connector import RedisConnector, HighSpeedPublisher

async def main():
    # Health monitoring starts automatically
    connector = RedisConnector(
        host="192.168.29.42",  # Replace with the actual IP of your Raspberry Pi
        port=6379,
        password="SuperDuperRedis6748@"
    )

    publisher = HighSpeedPublisher(connector)

    # 2. Mock exchange data
    mock_exchange_data = {
        "AAAAAA": 50000.12,
        "BBBBBB": 3000.45,
        "CCCCCC": 2045.00
    }

    print(f"Publishing {len(mock_exchange_data)} prices...")
    success = await publisher.publish_prices(mock_exchange_data)
    if success:
        print("✅ Publishing complete.")
    else:
        print("❌ Publishing failed (Redis unhealthy or congested).")

    await connector.close()

if __name__ == "__main__":
    asyncio.run(main())