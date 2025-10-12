import asyncio
from gmqtt import Client as GMqttClient, MQTTConnectError
import logging
from mqtt_connector_lib import constants



# HandlerFunc: TypeAlias = Callable[[str, str], Awaitable[None]]


class BrokerClient:
    def __init__(self,client_id, host, port):
        self.client_id = client_id
        self.host = host
        self.port = port


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

        # logger
        self.logger = logging.getLogger(constants.SERVICE_NAME)
        self.logger.info(f"GMQTT Connector is initialized with client id - {self.client_id}")

