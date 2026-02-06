"""
MQTT Connector Library - Redis Edition

High-performance Redis pub/sub connector for trading bots and real-time applications.
"""

from mqtt_connector_lib.redis_connector import (
    RedisConnector,
    HighSpeedPublisher,
    HighSpeedSubscriber
)

__all__ = [
    "RedisConnector",
    "HighSpeedPublisher",
    "HighSpeedSubscriber"
]

__version__ = "2.0.0"

