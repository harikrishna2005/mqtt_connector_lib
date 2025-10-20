

import asyncio
from gmqtt import Client as GMqttClient, MQTTConnectError
import socket

from mqtt_connector_lib.exceptions import MyMqttBaseError, MyMqttConnectionError

import logging
from mqtt_connector_lib import constants



# HandlerFunc: TypeAlias = Callable[[str, str], Awaitable[None]]
adapter_context = {'prefix': '[MQTT CONNECTOR]'}
# logger = logging.LoggerAdapter(logging.getLogger(constants.SERVICE_NAME), adapter_context)
logger = logging.getLogger(constants.SERVICE_NAME)
logger = logging.LoggerAdapter(logger, adapter_context)

class BrokerClient:
    def __init__(self,client_id, host, port, user_name=None, password=None):
        self.client_id = client_id
        self.host = host
        self.port = port
        self.user_name = user_name
        self.password = password


class GMqttConnector:
    def __init__(self, broker_details: BrokerClient,
                    clean_session=True
                 ):
        self.client_id = broker_details.client_id
        self.host = broker_details.host
        self.port = broker_details.port
        self.clean_session = clean_session
        self.client = GMqttClient(self.client_id, clean_session=clean_session)


        # State Management
        self._connected = asyncio.Event()
        self._processing_lock = asyncio.Lock()  # Ensures only one processor runs at a time

        # Assign gmqtt callbacks
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect

        # # Logger -  Create a LoggerAdapter to add the prefix
        # adapter_context = {'prefix': '[MQTT CONNECTOR]'}
        # self.logger1 = logging.LoggerAdapter(logger, adapter_context)


        logger.info(f"MQTT Client Initialied: {self.client_id}")


    async def connectAsync(self, username=None, password=None):
        #	| Scenario                               | Exception to Catch       |
        #	| -------------------------------------- | ------------------------ |
        #	| Invalid hostname                       | `socket.gaierror`        |
        #	| Valid IP, but port not open            | `ConnectionRefusedError` |
        #	| No route to host / Unreachable network | `OSError`                |
        #	| Broker does not respond with CONNACK   | `asyncio.TimeoutError`   |
        #	| Anything else                          | Generic `Exception`      |
        #
        # BaseException
        # └── Exception
        #    └── OSError
        #        └── ConnectionError
        #            └── MQTTConnectionError

        self._connected.clear()
        if username:
            self.client.set_auth_credentials(username, password)
        # logger.info(f"Attempting connection to MQTT broker: {self.host}:{self.port}")

        try:
            logger.info(f"Waiting for broker to connect.....  ")
            await self.client.connect(host=self.host, port=self.port, keepalive=60)
            await asyncio.wait_for(self._connected.wait(), timeout=30)

        except socket.gaierror as se:

            error_details = {
                "MQTT_CONNECTION_ERROR_DETAILS": {
                    "host": self.host,
                    "port": self.port,
                    "user_name": username if username else None,
                    "password_provided": True if password else False,
                    "client_id": self.client_id,
                }
            }
            connection_error = MyMqttConnectionError(
                message=f"Invalid host name: {self.host}",
                reason_code="dns_resolution_failed",
                **error_details
            )
            logger.error(f"{connection_error}")
            raise connection_error from se

        except ConnectionRefusedError as cre:

            error_details = {
                "MQTT_CONNECTION_ERROR_DETAILS": {
                    "host": self.host,
                    "port": self.port,
                    "user_name": username if username else None,
                    "password_provided": True if password else False,
                    "client_id": self.client_id,
                }
            }
            connection_error = MyMqttConnectionError(
                message=f"Invalid PORT: {self.port}",
                reason_code="dns_resolution_failed",
                **error_details
            )
            logger.error(f"{connection_error}")
            raise connection_error from cre

        except MQTTConnectError as mce:

            error_details = {
                "MQTT_CONNECTION_ERROR_DETAILS": {
                    "host": self.host,
                    "port": self.port,
                    "user_name": username if username else None,
                    "password_provided": True if password else False,
                    "client_id": self.client_id,
                }
            }
            connection_error = MyMqttConnectionError(
                message=f"{mce.message}",
                reason_code={mce._code},
                **error_details
            )
            logger.error(f"{connection_error}")
            raise connection_error from mce

        except asyncio.TimeoutError as te:
            error_details = {
                "MQTT_CONNECTION_ERROR_DETAILS": {
                    "host": self.host,
                    "port": self.port,
                    "user_name": username if username else None,
                    "password_provided": True if password else False,
                    "client_id": self.client_id,
                    "timeout_seconds": 30
                }
            }
            connection_error = MyMqttConnectionError(
                message=f"Broker did not respond within 30 seconds: {self.host}:{self.port}",
                reason_code="connection_timeout",
                **error_details
            )
            logger.error(f"{connection_error}")
            raise connection_error from te

        except OSError as ose:

            error_details = {
                "MQTT_CONNECTION_ERROR_DETAILS": {
                    "host": self.host,
                    "port": self.port,
                    "user_name": username if username else None,
                    "password_provided": True if password else False,
                    "client_id": self.client_id,
                }
            }
            connection_error = MyMqttConnectionError(
                message=f"The network location cannot be reached",
                reason_code="Might be INTERNET issue",
                **error_details
            )
            logger.error(f"{connection_error}")
            raise connection_error from ose

        except Exception as ex:

            error_details = {
                "MQTT_CONNECTION_ERROR_DETAILS": {
                    "host": self.host,
                    "port": self.port,
                    "user_name": username if username else None,
                    "password_provided": True if password else False,
                    "client_id": self.client_id,
                }
            }
            connection_error = MyMqttConnectionError(
                message=f"UN-EXPECTED EXCEPTION",
                reason_code="Analyse the issue",
                **error_details
            )
            logger.error(f"{connection_error}")
            raise connection_error from ex

    async def disconnectAsync(self):
        """
        Common Disconnect Scenarios:
            1. Graceful Disconnect ✅
                Normal operation
                Client sends DISCONNECT packet
                Broker acknowledges
                Connection closed cleanly
            2. Network Already Down ⚠️
                Network cable unplugged
                WiFi disconnected
                Raises OSError or ConnectionError
            3. Broker Already Closed Connection ⚠️
                Broker forced disconnect
                Keep-alive timeout
                Connection already broken
            4. Client Already Disconnected ⚠️
                Multiple disconnect calls
                Client was never connected
                Should handle gracefully
            5. Event Loop Issues ❌
                Event loop closed
                Client running in wrong loop
                Raises RuntimeError
        """
        try:
            logger.info(f"Disconnecting from broker {self.host}:{self.port}...")
            if not self._connected.is_set():
                logger.info(f"Client : {self.client_id} already disconnected")
                return
            await self.client.disconnect()
            logger.info("Successfully disconnected from broker")
        except OSError as ose:
            # Network unreachable, connection lost
            logger.warning(f"Network error during disconnect: {ose}")
            # self._connected.clear()  # Force clear state
        except ConnectionError as ce:
            # Connection already closed/broken
            logger.warning(f"Connection was already closed: {ce}")
        except socket.error as se:
            # Socket-level errors
            logger.warning(f"Socket error during disconnect: {se}")
        except Exception as ex:
            # Any other unexpected errors
            logger.error(f"Unexpected error during disconnect: {ex}")

        # logger.info("Disconnecting to broker")




    def _on_connect(self, client, flags, rc, properties):
        self._connected.set()

        logger.info(f"Connected/Re-connected to broker {self.host}:{self.port} (rc={rc})")

        # First time load-> load from persistence to in-memory
        # if self._first_time_load :
        #
        #
        #     # Load and subscribe
        #     # async for subscriber_topic in self._topic_store.get_all_topics_Async_Gen(  exception_occurred_messages = False):
        #     #     topic = subscriber_topic.topic
        #     #     qos = subscriber_topic.qos
        #     #     handler = self._topic_handlers.get(topic, self.default_handler)
        #     #
        #     #     # Subscribe to the topic
        #     #     try:
        #     #         await self.client.subscribe(topic, qos)
        #     #         logger.info(f"[GMQTT] Resubscribed to topic '{topic}' with qos={qos} with handler : {handler.__name__ if hasattr(handler, '__name__') else str(handler)}")
        #     #     except Exception as e:
        #     #         logger.error(f"[GMQTT] Failed to resubscribe to topic '{topic}': {e}")
        #     #         await self._topic_store.mark_failed_with_exception_async(topic=topic, exception_full=e)
        #
        #     # load pending publishers from persistence to in-memory
        #     # self._pending_publishers = self._topic_store.get_all_pending_publishers()
        #
        #     self._first_time_load = False   # make it false if all publishing and subscribing is done.
        #
        #


        # Resubscribe topics all from in-memory
        # code it

        # Retry pending publishes from in-memory
        # code it



    def _on_disconnect(self, client, packet, exc=None):
        self._connected.clear()
        if exc:
            logger.warning(f"Disconnected unexpectedly: {exc}")
        else:
            logger.info("Disconnected cleanly")