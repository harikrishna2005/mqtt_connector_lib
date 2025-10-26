from mqtt_connector_lib.exceptions import MyMqttConnectionError,MyMqttSubscriptionError
from mqtt_connector_lib.gmqtt_connector import BrokerClient,GMqttConnector
import asyncio
import logging
from initsetup import setup_logging
setup_logging()
adapter_context = {'prefix': '[APP_START]'}
logger = logging.getLogger("my_app_start")
logger = logging.LoggerAdapter(logger, adapter_context)
# import logging
# from contextlib import contextmanager, asynccontextmanager
# import contextvars
#
#
# # Context variables (thread-safe / async-safe)
# trace_id_var = contextvars.ContextVar("trace_id", default="-")
# span_id_var = contextvars.ContextVar("span_id", default="-")
#
# TRACE_HEADER = "X-Trace-Id"  # HTTP header for trace_id
#
# # ----------------------
# # Logger setup
# # ----------------------
# # class TraceIdFilter(logging.Filter):
# #     def filter(self, record):
# #         record.trace_id = getattr(record, 'trace_id', 'N/A')
# #         return True
#
# class ContextFilter(logging.Filter):
#     def filter(self, record: logging.LogRecord) -> bool:
#         record.trace_id = trace_id_var.get()
#         record.span_id = span_id_var.get()
#
#         return True
# file_formatter = logging.Formatter(
#         # "%(asctime)s %(levelname)s trace_id=%(trace_id)s span_id=%(span_id)s %(name)s [%(funcName)s:%(lineno)d]: %(message)s"
#         # "%(levelname)-8s %(asctime)s T%(trace_id)-20s S%(span_id)-20s %(name)-50s : %(message)s [%(funcName)s:%(lineno)d]"
#         "%(levelname)-8s %(asctime)s T%(trace_id)-20s S%(span_id)-20s [%(name)-50s] : %(message)s [%(funcName)s:%(lineno)d]"
#     )
#
# stream_handler = logging.StreamHandler()
# stream_handler.setFormatter(file_formatter)
# stream_handler.addFilter(ContextFilter())  # Ensure filter is on the handler
#
# root_logger = logging.getLogger()
# root_logger.setLevel(logging.INFO)
# root_logger.handlers.clear()
# root_logger.addHandler(stream_handler)


import gmqtt
from gmqtt import Client as MQTTClient
from gmqtt import Subscription # <-- Import this

live_mosquitto_broker = BrokerClient(
    host="test.mosquitto.org",
    port=1883,
    # host="XXX.mosquitto.org",
    # port=1999,
    client_id="test_client_12345")

aws_broker = BrokerClient(
    # host="test.mosquitto.org",
    host="13.127.243.114",
    port=1883,
    client_id="test_client_12345",
    user_name="mqtt_user",
    # user_name="hahahahaha_user",
    # password="My_mqtt_password629@?")
    password="My_mqtt_password629@?")


test_client = aws_broker

# connector = MqttClient(broker_details=test_client)

# Assuming you subscribe to multiple topics with a single call
# For example: client.subscribe([('topic1', 1), ('topic2', 2), ('topic3', 0)])

# test_client = live_mosquitto_broker
def on_subscribe(client, mid, granted_qos, properties):
    """
    Called when the broker acknowledges a subscription request.
    'granted_qos' is a list of integers (the return codes).
    """
    logger.info(f"Subscription Acknowledged. Message ID: {mid}")
    logger.info(f"Granted QoS levels/Return Codes: {granted_qos}")

    for qos_code in granted_qos:
        if qos_code == 128:
            print("❌ **Subscription Failed/Rejected** for one or more topics.")
            # Handle the failure, e.g., log it or raise an exception
            return  # Exit the function as a failure was found

    # If the loop finishes without finding 128
    logger.info("✅ **All Subscriptions Successful** (Granted QoS 0, 1, or 2).")


# --- Setup Client ---
client = MQTTClient("client-id-sub")
client.on_subscribe = on_subscribe


async def main():
    if test_client.user_name:
        client.set_auth_credentials(test_client.user_name, test_client.password)

    await client.connect(host=test_client.host, port=test_client.port)  # Replace with your broker

    # 1. Successful subscription example
    # Note: 'gmqtt' subscribe method returns the mid, which can be useful
    # but the status is checked in the *callback*.

    # Example 1: Successful subscriptions for three topics
    logger.info("Attempting to subscribe to 3 topics...")

    # Correct subscription format using gmqtt.Subscription
    sub_timer = 2
    async def timer_countdown(seconds):
        for i in range(seconds, 0, -1):
            logger.info(f"Waiting... {i} seconds remaining")
            await asyncio.sleep(1)

    # Start a separate asyncio task for the timer countdown
    asyncio.create_task(timer_countdown(sub_timer))
    logger.info("Attempting to subscribe to 3 topics...")
    await asyncio.sleep(sub_timer)
    subscriptions = [
        Subscription('my/topic/q1', qos=1),
        Subscription('my/topic/q2', qos=2),
        Subscription('my/topic/q0', qos=0)
    ]

    # Pass the list of Subscription objects to client.subscribe()
    mid = client.subscribe(subscriptions)
    # mid = client.subscribe([('my/topic/q1', 1), ('my/topic/q2', 2), ('my/topic/q0', 0)])
    logger.info(f"Subscription request sent (mid={mid}). Waiting for SUBACK...")

    longwait = 120
    logger.info(f"Waiting for {longwait} seconds before end of application")
    await asyncio.sleep(longwait)  # Give time for the SUBACK (on_subscribe) to be called
    logger.info(f"Waiting completed. Going to end the application.")

    # Example 2: To test a **failure**, you would typically need a broker
    # configured to reject a subscription (e.g., using an invalid topic or ACL restriction).
    # Since we can't guarantee a broker rejection here, the interpretation above
    # of the '128' code is the key.

    await client.disconnect()


# asyncio.run(main())

if __name__ == "__main__":
    # print("Starting MQTT client...")
    # mqtt_client_connector = GMqttConnector(broker_details=test_client)
    # mqtt_client_connector.connectAsync(username="randomuser", password="randompassword")
    #
    #
    # # mqtt_client_connector.connect()
    # print("MQTT client connected.")
    asyncio.run(main())