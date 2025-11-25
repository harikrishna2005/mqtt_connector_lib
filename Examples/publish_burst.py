import asyncio
from gmqtt import Client as superMQTTClient

BROKER = "broker.mqtt.cool"
PORT = 1883
TOPIC = "my_own_topic"

# gmqtt requires a unique client ID
CLIENT_ID = "burst-publisher-client"


def on_connect(client, flags, rc, properties):
    print("Connected:")


def on_disconnect(client, packet, exc=None):
    print("Disconnected")


async def my_publish_burst():
    client = superMQTTClient(CLIENT_ID)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    # Connect to TCP broker
    await client.connect(BROKER, PORT, keepalive=30)

    print("Publishing 100 messages as fast as possible...")

    for batch in range(1, 11):  # 10 times
        print(f"Publishing batch {batch}/10...")
        for i in range(1, 100):  # 50 messages (1-50)
            msg = f"gmqtt burst message #{i} (batch {batch})"
            client.publish(TOPIC, msg, qos=0)  # non-blocking, fastest
        await asyncio.sleep(0.5)  # Wait 1 second before next batch

    print("Published all messages. Waiting for network flush...")

    # Give gmqtt time to flush packets to the network
    await asyncio.sleep(1)

    await client.disconnect()
    print("Done!")


if __name__ == "__main__":
    asyncio.run(my_publish_burst())
