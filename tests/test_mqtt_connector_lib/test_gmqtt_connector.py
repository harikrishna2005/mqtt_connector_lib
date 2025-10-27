import pytest_asyncio
import pytest
import logging

import asyncio
import socket
import logging
from unittest.mock import AsyncMock, MagicMock, patch

# from gmqtt import MQTTConnectError

from mqtt_connector_lib.gmqtt_connector import BrokerClient, GMqttConnector
from mqtt_connector_lib.exceptions import MyMqttConnectionError,MyMqttSubscriptionError, MyMqttBaseError



logging.disable(logging.CRITICAL)


@pytest_asyncio.fixture
def mock_gmqtt_client():
    """Fixture to mock the gmqtt.Client instance."""
    with patch('src.mqtt_connector_lib.gmqtt_connector.GMqttClient') as MockClient:
        instance = MockClient.return_value
        instance.connect = AsyncMock()
        instance.disconnect = AsyncMock()
        instance.set_auth_credentials = MagicMock()
        yield instance


@pytest_asyncio.fixture
async def connector(mock_gmqtt_client):
    """Fixture to provide a GMqttConnector instance with a mocked client."""
    broker_details = BrokerClient(
        client_id="test-client",
        host="localhost",
        port=1883
    )
    conn = GMqttConnector(broker_details=broker_details,
                          clean_session=True)
    # Replace the real client with our mock
    conn.client = mock_gmqtt_client
    return conn

@pytest_asyncio.fixture
async def connected_connector(connector):
    """A connector that is already in a 'connected' state."""
    connector._connected.set()
    return connector


# Test Cases

@pytest.mark.asyncio
async def test_connect_successful(connector, mock_gmqtt_client):
    """
    Scenario 1: Test a successful connection to the broker.
    """
    print("Running: test_connect_successful")
    # Arrange: Simulate the on_connect callback being called successfully
    async def side_effect(*args, **kwargs):
        connector._on_connect(None, None, 0, None)
        return await asyncio.sleep(0)
    mock_gmqtt_client.connect.side_effect =  side_effect

    # Act
    await connector.connectAsync("user", "pass")
    # assert mqtt_client_connector._connected.is_set() is True

    # Assert
    # mock_gmqtt_client.set_auth_credentials.assert_called_with("user", "pass")
    # mock_gmqtt_client.connect.assert_awaited_with(host="localhost", port=1883, keepalive=60)
    assert connector._connected.is_set() is True
    print("Success: test_connect_successful")

@pytest.mark.asyncio
async def test_connect_no_internet_or_network_error(connector, mock_gmqtt_client):
    """
    Scenario 3 & 4: Test connection failure due to a network error (e.g., no internet).
    """
    # print("Running: test_connect_no_internet_or_network_error")
    # Arrange
    mock_gmqtt_client.connect.side_effect = OSError("Network is unreachable")

    # Act & Assert
    with pytest.raises(MyMqttConnectionError) as excinfo:
        await connector.connectAsync()

    assert "The network location cannot be reached" in excinfo.value.message
    assert excinfo.value.reason_code == "Might be INTERNET issue"
    assert connector._connected.is_set() is False
    # print("Success: test_connect_no_internet_or_network_error")

@pytest.mark.asyncio
async def test_INTEGRATION_connect_successful():
    test_client = BrokerClient(
        host="test.mosquitto.org",
        port=1883,
        # host="XXX.mosquitto.org",
        # port=1999,
        client_id="test_client_12345")
    mqtt_client_connector = GMqttConnector(broker_details=test_client)
    await mqtt_client_connector.connectAsync(username=test_client.user_name, password=test_client.password)
    assert mqtt_client_connector._connected.is_set() is True


@pytest.mark.asyncio
async def test_INTEGRATION_Invalid_host_address():
    test_client = BrokerClient(
        # host="test.mosquitto.org",
        port=1883,
        host="XXX.mosquitto.org",
        # port=1999,
        client_id="test_client_12345")
    mqtt_client_connector = GMqttConnector(broker_details=test_client)
    # await mqtt_client_connector.connectAsync(username=test_client.user_name, password=test_client.password)
    # Assert that MyMqttConnectionError is raised
    with pytest.raises(MyMqttConnectionError, match="Invalid host name") as exc_info:
        await mqtt_client_connector.connectAsync(
            username=test_client.user_name,
            password=test_client.password
        )
    assert mqtt_client_connector._connected.is_set() is False

@pytest.mark.asyncio
async def test_INTEGRATION_Invalid_Port():
    test_client = BrokerClient(
        # host="test.mosquitto.org",
        port=2999,
        host="test.mosquitto.org",
        # port=1999,
        client_id="test_client_12345")
    mqtt_client_connector = GMqttConnector(broker_details=test_client)
    # await mqtt_client_connector.connectAsync(username=test_client.user_name, password=test_client.password)
    # Assert that MyMqttConnectionError is raised
    with pytest.raises(MyMqttConnectionError, match="Invalid PORT") as exc_info:
        await mqtt_client_connector.connectAsync(
            username=test_client.user_name,
            password=test_client.password
        )
    assert mqtt_client_connector._connected.is_set() is False

@pytest.mark.asyncio
async def test_INTEGRATION_Invalid_Credentials():
    test_client = BrokerClient(
        port=1883,
        host="test.mosquitto.org",
        client_id="test_client_12345",
        user_name="invalid_user",
        password="invalid_pass"
    )
    mqtt_client_connector = GMqttConnector(broker_details=test_client)

    with pytest.raises(MyMqttConnectionError, match="Not authorized") as exc_info:
        await mqtt_client_connector.connectAsync(
            username=test_client.user_name,
            password=test_client.password
        )
    await asyncio.sleep(1)
    assert mqtt_client_connector._connected.is_set() is False


@pytest.mark.asyncio
async def test_disconnect_network_error(connector, mock_gmqtt_client):
    """Test disconnect with network error."""
    mock_gmqtt_client.disconnect.side_effect = OSError("Network unreachable")

    # Should not raise exception, just log warning
    await connector.disconnectAsync()
    assert connector._connected.is_set() is False

@pytest.mark.asyncio
async def test_INTEGRATION_Already_disconnected():
    test_client = BrokerClient(
        host="test.mosquitto.org",
        port=1883,
        # host="XXX.mosquitto.org",
        # port=1999,
        client_id="test_client_12345")
    mqtt_client_connector = GMqttConnector(broker_details=test_client)
    await mqtt_client_connector.connectAsync(username=test_client.user_name, password=test_client.password)
    await mqtt_client_connector.disconnectAsync()
    await mqtt_client_connector.disconnectAsync()
    assert mqtt_client_connector._connected.is_set() is False




class TestSubscription:

    @pytest_asyncio.fixture
    async def mock_connected_connector(self):
        broker_details = BrokerClient(
            host="localhost",
            port=1883,
            # host="XXX.mosquitto.org",
            # port=1999,
            client_id="test_client_12345")

        with patch('mqtt_connector_lib.gmqtt_connector.GMqttClient') as MockClient:
            instance = MockClient.return_value
            instance.subscribe = MagicMock()
            instance.unsubscribe = MagicMock()

            conn = GMqttConnector(broker_details=broker_details, clean_session=True)
            conn.client = instance
            conn._connected.set()  # Mark as connected
            yield conn

    @pytest_asyncio.fixture
    async def real_connected_connector(self):
        broker_details = BrokerClient(
            host="test.mosquitto.org",
            port=1883,
            # host="XXX.mosquitto.org",
            # port=1999,
            client_id="test_client_12345")

        # with patch('mqtt_connector_lib.gmqtt_connector.GMqttClient') as MockClient:
        #     instance = MockClient.return_value
        #     instance.subscribe = AsyncMock()
        #     instance.unsubscribe = AsyncMock()
        #
        #     conn = GMqttConnector(broker_details=broker_details, clean_session=True)
        #     conn.client = instance
        #     conn._connected.set()  # Mark as connected
        #     yield conn
        conn = GMqttConnector(broker_details=broker_details, clean_session=True)
        await conn.connectAsync(username=broker_details.user_name, password=broker_details.password)
        #     conn.client = instance
        #     conn._connected.set()  # Mark as connected
        yield conn
        await conn.disconnectAsync()

    # @pytest.mark.asyncio
    # async def test_subscribe_async_success(self, mock_connected_connector):
    #     """
    #     Tests the success scenario for subscribeAsync using a mocked client.
    #     """
    #     # Arrange
    #     connector = mock_connected_connector
    #     mock_gmqtt_client = connector.client
    #     test_mid = 1
    #     test_topic = "test/success"
    #     test_qos = 1
    #
    #     # Configure the mock to return a specific message ID
    #     mock_gmqtt_client.subscribe.return_value = test_mid
    #
    #     async def dummy_message_handler(topic, payload):
    #         pass
    #
    #     try:
    #         # Act
    #         # Start the subscribe task but don't wait for it to complete yet.
    #         # It will block waiting for the _on_subscribe event.
    #         subscribe_task = asyncio.create_task(
    #             connector.subscribeAsync(
    #                 topic=test_topic,
    #                 handler=dummy_message_handler,
    #                 qos=test_qos
    #             )
    #         )
    #
    #         # Give the task a moment to run and add the mid to _pending_subscriptions
    #         await asyncio.sleep(0.01)
    #
    #         # Now, simulate the broker sending a SUBACK by calling the callback.
    #         # This will set the event that the subscribe_task is waiting for.
    #         granted_qos = [test_qos]
    #         connector._on_subscribe(mock_gmqtt_client, test_mid, granted_qos, {})
    #
    #         # Await the task to ensure it completes without errors.
    #         await subscribe_task
    #
    #         # Assert
    #         # Verify that the underlying client's subscribe method was called correctly.
    #         # mock_gmqtt_client.subscribe.assert_called_once_with(test_topic, qos=test_qos)
    #         mock_gmqtt_client.subscribe.assert_called_once_with(test_topic, test_qos)
    #         # If we reached here without a timeout or other exception, the test is successful.
    #         assert True
    #
    #     except MyMqttSubscriptionError as e:
    #         pytest.fail(f"subscribeAsync raised an unexpected exception on success: {e}")

    @pytest.mark.asyncio
    async def test_INTEGRATION_subscribe_success_path_is_executed(self, real_connected_connector, caplog):
        """
        Verifies that the success logic inside _on_subscribe is executed
        by checking for the specific log message.
        """
        # Arrange
        connector = real_connected_connector
        test_topic = "test/success/path"
        test_qos = 1

        async def dummy_handler(topic, payload):
            pass

        # Act
        # Set the log level to INFO to capture the desired message
        with caplog.at_level(logging.INFO):
            await connector.subscribeAsync(
                topic=test_topic,
                handler=dummy_handler,
                qos=test_qos
            )
            await asyncio.sleep(1)

        # Assert
        # Check if the expected success message is present in the captured logs.
        assert "subscription SUCCESS for (mid=" in caplog.text

        # Optional: Clean up the subscription
        # await connector.unsubscribeAsync([test_topic])

    @staticmethod
    async def dummy_handler_one(topic, payload):
        pass

    @staticmethod
    async def dummy_handler_two(topic, payload):
        pass

    @pytest.mark.parametrize(
        "test_topic,test_qos, my_test_handler",
        [
            ("integration/test/handler_one",0, dummy_handler_one),
            ("integration/test/handler_two",1, dummy_handler_two),
            ("another/topic/path",2, dummy_handler_one),  # Example of re-using a handler
            ("integration/test/handler_one",0, dummy_handler_two),  # updating the same topic with different handler
        ]
    )
    @pytest.mark.asyncio
    async def test_INTEGRATON_subscribe_stores_in_memory_topics_with_handlers(self, real_connected_connector, test_topic, test_qos,my_test_handler):
        """
        Verifies that after a successful subscription, the topic and its handler
        are correctly stored in the _topic_handlers dictionary.
        """
        # Arrange
        connector = real_connected_connector
        # test_topic = "integration/test/handler"
        test_qos = 1
        on_subscribe_completed = asyncio.Event()

        # async def my_test_handler(topic, payload):
        #     pass

        # Patch the 'on_subscribe' callback on the actual gmqtt client instance
        original_on_subscribe = connector.client.on_subscribe

        def side_effect_on_subscribe(*args, **kwargs):
            # Call the original callback logic
            original_on_subscribe(*args, **kwargs)
            # Signal that the callback has completed
            on_subscribe_completed.set()

        # Replace the callback on the client, not just the method on the connector
        connector.client.on_subscribe = side_effect_on_subscribe

        # Act
        # Call subscribeAsync to initiate the subscription process.
        await connector.subscribeAsync(
            topic=test_topic,
            handler=my_test_handler,
            qos=test_qos
        )

        # Wait for the _on_subscribe callback to finish.
        await asyncio.wait_for(on_subscribe_completed.wait(), timeout=5)

        # Assert
        # Check that the topic exists as a key in the dictionary.
        assert test_topic in connector._topic_handlers
        # Check that the value for the key is the handler function we provided.
        assert connector._topic_handlers[test_topic] is my_test_handler

    @pytest.mark.asyncio
    async def test_INTEGRATION_subscribe_without_handler(self, real_connected_connector):
        with pytest.raises(MyMqttSubscriptionError, match="No Handler provided during subscription") as exc_info:
            await real_connected_connector.subscribeAsync(
                topic="test/topic",
                handler=None,
                qos=1
            )
            await asyncio.sleep(1)

