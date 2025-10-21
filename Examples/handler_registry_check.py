import asyncio

from mqtt_connector_lib.interfaces import  IHandlerRegistry
from mqtt_connector_lib.handler_registry import HandlerRegistry
from mqtt_connector_lib.handlers import PrintMessageHandler
from mqtt_connector_lib.exceptions import HandlerNotFoundError, InvalidHandlerError, HandlerRegistryError

import logging
from initsetup import setup_logging
setup_logging()
adapter_context = {'prefix': '[APP_START]'}
logger = logging.getLogger("my_app_start")
logger = logging.LoggerAdapter(logger, adapter_context)

async def super_async():
    print(f"Inside super async function")


async def main():
    logger.info("Starting the application")
    handler_registry : IHandlerRegistry  = HandlerRegistry()
    print_message_handler = PrintMessageHandler()
    handler_registry.register_handler(print_message_handler.handle_messageAsync)


    try:
        handler_registry.register_handler(super_async)
    except HandlerRegistryError as e:
        logger.exception(f"Unable to register handler: {str(e)}")



    logger.info("Appliation ended")




if __name__ == "__main__":
    # print("Starting MQTT client...")
    # mqtt_client_connector = GMqttConnector(broker_details=test_client)
    # mqtt_client_connector.connectAsync(username="randomuser", password="randompassword")
    #
    #
    # # mqtt_client_connector.connect()
    # print("MQTT client connected.")
    asyncio.run(main())