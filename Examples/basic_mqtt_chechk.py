from mqtt_connector_lib.exceptions import MyMqttConnectionError
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

live_mosquitto_broker = BrokerClient(
    host="test.mosquitto.org",
    port=1883,
    # host="XXX.mosquitto.org",
    # port=1999,
    client_id="test_client_12345")

aws_broker = BrokerClient(
    # host="test.mosquitto.org",
    host="13.126.108.152",
    port=1883,
    client_id="test_client_12345",
    user_name="mqtt_user",
    # user_name="hahahahaha_user",
    # password="My_mqtt_password629@?")
    password="zzzzzzzzzzzzzz_mqtt_password629@?")


test_client = live_mosquitto_broker
# test_client = aws_broker

# connector = MqttClient(broker_details=test_client)



async def main():
    logger.info("Starting the application")
    mqtt_client_connector = GMqttConnector(broker_details=test_client)
    # await mqtt_client_connector.connectAsync(username="randomuser", password="randompassword")
    try :
        await mqtt_client_connector.connectAsync(username=test_client.user_name, password=test_client.password)
    except MyMqttConnectionError as me:
        logger.exception(f"Unable to connect to broker: {me}")
        # logger.exception(me)

        return


    # mqtt_client_connector.connect()
    await asyncio.sleep(20)
    print("MQTT client connected.")




if __name__ == "__main__":
    # print("Starting MQTT client...")
    # mqtt_client_connector = GMqttConnector(broker_details=test_client)
    # mqtt_client_connector.connectAsync(username="randomuser", password="randompassword")
    #
    #
    # # mqtt_client_connector.connect()
    # print("MQTT client connected.")
    asyncio.run(main())