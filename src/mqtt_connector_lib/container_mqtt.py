from dependency_injector import containers, providers
from src.mqtt_connector_lib.handler_registry import HandlerRegistry


class MqttConnectorContainer(containers.DeclarativeContainer):
    """Dependency injection container for MQTT connector."""
    handler_registry = providers.Singleton(HandlerRegistry)

    # config = providers.Configuration()
    #
    # mqtt_connector = providers.Singleton(
    #     lambda: MqttConnector(
    #         host=config.mqtt.host,
    #         port=config.mqtt.port,
    #         username=config.mqtt.username,
    #         password=config.mqtt.password,
    #     )
    # )

mqttContainer = MqttConnectorContainer()