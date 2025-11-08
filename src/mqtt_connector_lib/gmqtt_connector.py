import time
from typing import Callable, Awaitable, Any
import asyncio
import socket
from gmqtt import Client as GMqttClient, MQTTConnectError

from mqtt_connector_lib.container_mqtt import mqttContainer
from mqtt_connector_lib.exceptions import MyMqttBaseError, MyMqttConnectionError, MyMqttSubscriptionError
from mqtt_connector_lib.interfaces import HandlerFunc
from mqtt_connector_lib.on_message_handler_executor import OnMessageHandlerExecutor

import logging
from mqtt_connector_lib import constants

adapter_context = {'prefix': constants.GMQTT_CONNECTOR_PREFIX}
# logger = logging.LoggerAdapter(logging.getLogger(constants.SERVICE_NAME), adapter_context)
logger = logging.getLogger(constants.SERVICE_NAME)
logger = logging.LoggerAdapter(logger, adapter_context)


class BrokerClient:
    def __init__(self, client_id, host, port, user_name=None, password=None):
        self.client_id = client_id
        self.host = host
        self.port = port
        self.user_name = user_name
        self.password = password





class GMqttConnector:
    """
    Information
    -----------------
    - During subscription, if no handler is provided, a MyMqttSubscriptionError is raised.

    Exceptions raised
    -----------------
    - MyMqttConnectionError: For connection related issues
    - MyMqttSubscriptionError: For subscription related issues including missing handlers
    - MyMqttBaseError: For other general MQTT errors


    """

    def __init__(self, broker_details: BrokerClient,
                 clean_session=True,
                 max_on_message_handler_workers=5
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
        self.client.on_subscribe = self._on_subscribe
        self.client.on_unsubscribe = self._on_unsubscribe
        self.client.on_message = self._on_message




        # Topics and its handlers # Handler Management
        self._topic_handlers : dict[str, HandlerFunc] = {}  # topic -> handler   => used on _on_message callback
        # self._topics_handlers_counters ={}   # topic -> counter    => we will use this counter during un-subscribe to remove topics and its handlers
        self._waiting_for_confirmation_subscriptions : dict[str, dict[str, Any]]= {}  # store mapping between mid -> topic names   =>  [mid] = {topic, handler, qos}

        self._handler_registry = mqttContainer.handler_registry()
        # self._user_friendly_handlers: dict[str, HandlerFunc] = {}  # consumer app accessibility

        # _on_message handler execution
        self._on_message_handler_executor = OnMessageHandlerExecutor(max_workers=max_on_message_handler_workers)

        logger.info(f"MQTT Client Initialied: {self.client_id}")


    async def registerHandlerFunctionAsync(self, handler_func: HandlerFunc, user_friendly_name:str =None) -> str:
        # self._user_friendly_handlers[user_friendly_name] = handler_func
        return self._handler_registry.register_handler(handler_func, user_friendly_name)  # Adding the handler to registry

    async def unRegisterHandlerFunctionAsync(self, user_friendly_name: str) -> bool:
        return self._handler_registry.unregister_handler(user_friendly_name)

    async def getHandlerFunctionAsync(self, user_friendly_name: str) -> HandlerFunc:
        return self._handler_registry.get_handler(user_friendly_name)

    async def getHandlerIdAsync(self, handler_func: HandlerFunc) -> str:
        return self._handler_registry.get_handler_id(handler_func)


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
            # logger.info(f"Below self.client.connect and going to wait for connected wait event ")
            await asyncio.wait_for(self._connected.wait(), timeout=30)
            # logger.info(f"after asyncio wait for connected event     ")

            # Start _on_message handler executor after successful connection
            await self._on_message_handler_executor.start()


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
                reason_code="connection_refused",
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
            # On Windows, invalid port can raise OSError with WinError 121 (semaphore timeout)
            # or WinError 10061 (connection refused)
            # Check if it's a port-related issue vs network issue
            is_port_error = False
            if hasattr(ose, 'winerror'):
                # Windows-specific error codes for connection issues
                if ose.winerror in [121, 10061, 10060]:  # Semaphore timeout, Connection refused, Timeout
                    is_port_error = True
            elif isinstance(ose, ConnectionRefusedError):
                is_port_error = True

            error_details = {
                "MQTT_CONNECTION_ERROR_DETAILS": {
                    "host": self.host,
                    "port": self.port,
                    "user_name": username if username else None,
                    "password_provided": True if password else False,
                    "client_id": self.client_id,
                }
            }

            if is_port_error:
                connection_error = MyMqttConnectionError(
                    message=f"Invalid PORT: {self.port}",
                    reason_code="invalid_port",
                    **error_details
                )
            else:
                connection_error = MyMqttConnectionError(
                    message=f"The network location cannot be reached",
                    reason_code="network_unreachable",
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
            # Stop _on_message handler executor before disconnect
            await self._on_message_handler_executor.stop()

            await self.client.disconnect()
            logger.info("Successfully disconnected from broker")
        except ConnectionError as ce:
            # Connection already closed/broken
            logger.warning(f"Connection was already closed: {ce}")
        except OSError as ose:
            # Network unreachable, connection lost
            logger.warning(f"Network error during disconnect: {ose}")
            # self._connected.clear()  # Force clear state
        except socket.error as se:
            # Socket-level errors
            logger.warning(f"Socket error during disconnect: {se}")
        except Exception as ex:
            # Any other unexpected errors
            logger.error(f"Unexpected error during disconnect: {ex}")

        # logger.info("Disconnecting to broker")

    def _on_connect(self, client, flags, rc, properties):
        self._connected.set()
        extra_details = {"Additional_details ": {
            "Session already present ": True if flags else False,
            "Protocol version": self.client.protocol_version
        }}

        logger.info(f"Connected/Re-connected to broker {self.host}:{self.port} (rc={rc}) ==> {extra_details}")

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

    def _on_subscribe(self, client, mid, granted_qos, properties):
        """
        Called when the broker acknowledges a subscription request.
        'granted_qos' is a list of integers (the return codes).
        """

        # Retrieve the topic associated with this mid
        # topic = self._pending_subscriptions.pop(mid, None)
        time.sleep(0.02)
        topic_details = self._waiting_for_confirmation_subscriptions.pop(mid, None)
        # topic_details = self._pending_subscriptions.get(mid, None)

        topic_local= "mid is not stored"
        handler_local = None
        requested_qos_local = 0

        if topic_details is not None:
            topic_local = topic_details['topic']
            handler_local = topic_details['handler']
            requested_qos_local = topic_details['qos']


        logger.info(f"Subscription Acknowledged. Message ID: {mid}  Granted QoS levels :  {granted_qos}")
        for qos_code in granted_qos:
            if  qos_code < 128: # 0, 1, 2 are success codes
                logger.info(f"subscription SUCCESS for (mid={mid}  topic = {topic_local}     Requested_QoS={requested_qos_local}     handler : {handler_local.__name__ if hasattr(handler_local, '__name__') else str(handler_local)} )   Granted QoS: {qos_code} ")
                # store the subscription details in persistence store if needed

                # TODO, -PERSISTANCE-  save in persistence the topic, qos and handler details for future use(when disconnection and reconnection happens)
                # store the topic and handler in the in-memory dictionary
                self._topic_handlers[topic_local] = handler_local
                # if topic_details is not None:
                #     self._store_in_memory_subscription(topic_local, handler_local ,granted_qos)


            else:
                logger.error(f"subscription Failed for (mid={mid} ")

                # (mid={mid}  topic = {topic} qos={qos} handler : {handler.__name__ if hasattr(handler, '__name__') else str(handler)} ).

    def _on_unsubscribe(self, client, mid, granted_qos):
        time.sleep(0.02)
        topic_details = self._waiting_for_confirmation_subscriptions.pop(mid, None)

        topic_local = "mid is not stored"
        handler_local = None
        requested_qos_local = 0

        if topic_details is not None:
            topic_local = topic_details['topic']
            handler_local = topic_details['handler']
            requested_qos_local = topic_details['qos']

        logger.info(f"Unsubscription Acknowledged. Message ID: {mid} for topic : {topic_local} granted QoS levels :  {granted_qos}")

        for qos_code in granted_qos:
            if qos_code < 128:
                # Remove from in-memory dict
                self._topic_handlers.pop(topic_local, None)
                # TODO, -PERSISTANCE- remove the subscribed topic from persistence store
                logger.info(f"Unsubscription SUCCESS for (mid={mid} topic={topic_local}) Granted QoS: {qos_code}")

            else:
                # TODO, -PERSISTANCE- Add to processing for failure un-subscription topic
                logger.error(f"Unsubscription FAILED for (mid={mid} topic={topic_local}) Granted QoS: {qos_code}")

        # logger.info(f"Subscription Acknowledged. Message ID: {mid}  Granted QoS levels :  {granted_qos}")

    def _on_message(self, client, topic, payload, qos, properties):
        # logger.info(f"zzzzzzzzzzzzzzzzzzzz message receive in on_message callback  topic: {topic}   payload: {payload}   qos: {qos} ")
        handler = self._topic_handlers.get(topic, None)
        # handler(topic, payload)  # - Raw Payload
        # Execute handler asynchronously instead of direct call
        self._on_message_handler_executor.execute_on_message_handler(topic, payload, handler)


    # def _store_in_memory_subscription(self, topic: str, handler: HandlerFunc, granted_qos: int):
    #     # Store the topic and handler in the in-memory dictionary
    #     # topic_details = self._pending_subscriptions.pop(mid, None)
    #     if handler is None:
    #         pass
    #
    #     self._topic_handlers[topic] = handler




    async def subscribeAsync(self, topic: str, handler: HandlerFunc, qos: int = 0):

        # If broker is down, log a warning and return without subscribing
        if not self._connected.is_set():
            logger.warning(
                f"The MQTT broker is down.  The topic : {topic} with qos : {qos} handler : {handler.__name__ if hasattr(handler, '__name__') else str(handler)} will be re-subscribed when broker is back")
            # save in persistence for re-subscription when broker is back
            # TODO, -PERSISTANCE- save in persistence for re-subscription when broker is back
            return



        error_details = {
            "MQTT_SUBSCRIPTION_ERROR_DETAILS": {
                "host": self.host,
                "port": self.port,
                "client_id": self.client_id,
                "topic": topic,
                "qos": qos,
            }
        }
        if handler is None:
            error_details["MQTT_SUBSCRIPTION_ERROR_DETAILS"]["handler_provided"] = False
            error_details["MQTT_SUBSCRIPTION_ERROR_DETAILS"][
                "How_to_fix"] = "Pass the handler parameter with a valid callable function"
            handler_exception = MyMqttSubscriptionError(message="No Handler provided during subscription",
                                                        reason_code=None,
                                                        **error_details)
            logger.error(f"No Handler provided during subscription to topic '{topic}' ==> {handler_exception}")
            raise handler_exception

        self._handler_registry.register_handler(handler)








        mid = self.client.subscribe(topic, qos)

        # Store the topic names against the mid for later lookup in the _on_subscribe callback
        # self._pending_subscriptions[mid] = {"topic" : topic, "handler": handler.__name__ if hasattr(handler, '__name__') else str(handler), "qos": qos}
        self._waiting_for_confirmation_subscriptions[mid] = {"topic" : topic, "handler": handler, "qos": qos}

        # logger.info(f"Subscribed to topic '{topic}' with qos={qos} and handler : {handler.__name__ if hasattr(handler, '__name__') else str(handler)}")
        logger.info(f"Subscription request sent (mid={mid}  topic = {topic} qos={qos} handler : {handler.__name__ if hasattr(handler, '__name__') else str(handler)} ). Waiting for SUBACK...")


    async def unsubscribeAsync(self, topic: str):
        """

        Args:
            topic:

        Returns:

        Scenarios:
            1. if broker is up
                a) successful Un-subscription
                b) Unsubscription when there is no topic
                c) Multiple Un-subscriptions for same topic
                d) Multiple un-subscriptions for different topic
                e) what ever the exception occurred during Un-subscription it should not break the code  or stop


            2. if broker is down
                a) successful Un-subscription
                b) Unsubscription when there is no topic
                c) Multiple Un-subscriptions for same topic
                d) Multiple subscriptions for different topic
                e) what ever the exception occurred during Un-subscription it should not break the code  or stop

        """

        # If broker is down, log a warning and return without unsubscribing
        if not self._connected.is_set():
            # self.client.is_connected
            logger.warning(
                f"[GMQTT] The MQTT broker is down.  The topic : {topic}  will be UN-subscribed when broker is back")
            # TODO, -PERSISTANCE- save in persistence for un-subscription when broker is back
            return

        try:
            mid = self.client.unsubscribe(topic)
            self._waiting_for_confirmation_subscriptions[mid] = {"topic": topic, "handler": None, "qos": None}
            # TODO, -PERSISTANCE- remove the unsubscribed topic from persistence store
            logger.info(f"Unsubscription request sent for topic '{topic}' (mid={mid}). Waiting for UNSUBACK...")
        except Exception as e:
            error_details = {
                "MQTT_UNSUBSCRIPTION_ERROR_DETAILS": {
                    "host": self.host,
                    "port": self.port,
                    "client_id": self.client_id,
                    "topic": topic,
                    "exception_full": str(e)
                }
            }
            unsubscription_error = MyMqttSubscriptionError(
                message=f"Failed to unsubscribe from topic '{topic}': {e}",
                reason_code="unsubscription_failed",
                **error_details
            )
            logger.error(f"Un-expected exception occured during unsubscribe : {unsubscription_error}")
            # TODO, - store this unsubscription failure topic in persistence for future processing -
            # Raise the exception to notify the caller
            raise unsubscription_error from e


    async def publishAsync(self, topic: str, payload: str, qos: int = 0, retain: bool = False):
        if not self._connected.is_set():
            logger.warning(
                f"The MQTT broker is down.  The topic : {topic} with payload : {payload} will be re-published when broker is back")
            # TODO, -PERSISTANCE- save in persistence for re-publishing when broker is back
            return

        try:
            self.client.publish(topic, payload, qos, retain)
            # logger.debug(f"Published message to topic '{topic}' with payload: {payload}, qos=(q{qos}), retain={retain}")
        except Exception as e:
            error_details = {
                "MQTT_PUBLISH_ERROR_DETAILS": {
                    "host": self.host,
                    "port": self.port,
                    "client_id": self.client_id,
                    "topic": topic,
                    "payload": payload,
                    "qos": qos,
                    "retain": retain,
                    "exception_full": str(e)
                }
            }
            publish_error = MyMqttBaseError(
                message=f"Failed to publish to topic '{topic}': {e}",
                reason_code="publish_failed",
                **error_details
            )
            logger.error(f"Un-expected exception occured during publish : {publish_error}")
            # TODO, - store this publish failure topic in persistence for future processing -
            raise publish_error from e