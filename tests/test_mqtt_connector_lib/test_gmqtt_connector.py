import pytest_asyncio
import pytest
import logging

import asyncio
import socket
import logging
from unittest.mock import AsyncMock, MagicMock, patch

# from gmqtt import MQTTConnectError

from mqtt_connector_lib.gmqtt_connector import BrokerClient, GMqttConnector
from mqtt_connector_lib.exceptions import MyMqttConnectionError, MyMqttBaseError



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
