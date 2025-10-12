from mqtt_connector_lib.gmqtt_connector import BrokerClient,GMqttConnector

import logging
from contextlib import contextmanager, asynccontextmanager
import contextvars


# Context variables (thread-safe / async-safe)
trace_id_var = contextvars.ContextVar("trace_id", default="-")
span_id_var = contextvars.ContextVar("span_id", default="-")

TRACE_HEADER = "X-Trace-Id"  # HTTP header for trace_id

# ----------------------
# Logger setup
# ----------------------
# class TraceIdFilter(logging.Filter):
#     def filter(self, record):
#         record.trace_id = getattr(record, 'trace_id', 'N/A')
#         return True

class ContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.trace_id = trace_id_var.get()
        record.span_id = span_id_var.get()

        return True
file_formatter = logging.Formatter(
        # "%(asctime)s %(levelname)s trace_id=%(trace_id)s span_id=%(span_id)s %(name)s [%(funcName)s:%(lineno)d]: %(message)s"
        # "%(levelname)-8s %(asctime)s T%(trace_id)-20s S%(span_id)-20s %(name)-50s : %(message)s [%(funcName)s:%(lineno)d]"
        "%(levelname)-8s %(asctime)s T%(trace_id)-20s S%(span_id)-20s [%(name)-50s] : %(message)s [%(funcName)s:%(lineno)d]"
    )

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(file_formatter)
stream_handler.addFilter(ContextFilter())  # Ensure filter is on the handler

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.handlers.clear()
root_logger.addHandler(stream_handler)

test_client = BrokerClient(
    # host="test.mosquitto.org",
    host="XXXXXX.mosquitto.org",
    port=1883,
    client_id="test_client_12345")


# connector = MqttClient(broker_details=test_client)


if __name__ == "__main__":
    print("Starting MQTT client...")
    mqtt_client_connector = GMqttConnector(broker_details=test_client)

    # mqtt_client_connector.connect()
    print("MQTT client connected.")