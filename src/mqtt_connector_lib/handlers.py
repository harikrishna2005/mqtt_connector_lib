from mqtt_connector_lib.interfaces import IMessageHandler

class PrintMessageHandler(IMessageHandler):
    async def handle_messageAsync(self, topic: str, payload: str):
        print(f"[PRINT MQTT MESSAGE] Topic: {topic} | Payload: {payload}")