import pytest_asyncio
import pytest
import logging

from mqtt_connector_lib.exceptions import InvalidHandlerError

logging.disable(logging.CRITICAL)


from mqtt_connector_lib.handler_registry import HandlerRegistry
from mqtt_connector_lib.handlers import PrintMessageHandler
@pytest_asyncio.fixture
def handler_registry():
    return HandlerRegistry()


def test_get_handler_id(handler_registry):
    """Test that get_handler_id generates consistent IDs for handler functions"""
    print_message_handler = PrintMessageHandler()
    handler_func = print_message_handler.handle_messageAsync

    # Get handler ID
    handler_id = handler_registry.get_handler_id(handler_func)

    # Handler ID should be a non-empty string
    assert isinstance(handler_id, str)
    assert len(handler_id) > 0


def test_get_handler(handler_registry):
    """Test that get_handler retrieves registered handlers correctly"""
    print_message_handler = PrintMessageHandler()
    handler_func = print_message_handler.handle_messageAsync

    # Register a handler first
    handler_id = handler_registry.register_handler(handler_func)

    # Test getting existing handler
    retrieved_handler = handler_registry.get_handler(handler_id)
    assert retrieved_handler is not None
    assert retrieved_handler == handler_func

    # Test getting non-existent handler
    non_existent_handler = handler_registry.get_handler("non_existent_id")
    assert non_existent_handler is None

    # Test with empty string
    empty_handler = handler_registry.get_handler("")
    assert empty_handler is None

    # Test with multiple handlers
    async def another_handler(topic: str, payload: str):
        pass

    another_id = handler_registry.register_handler(another_handler)

    # Verify we can get both handlers independently
    first_retrieved = handler_registry.get_handler(handler_id)
    second_retrieved = handler_registry.get_handler(another_id)

    assert first_retrieved == handler_func
    assert second_retrieved == another_handler
    assert first_retrieved != second_retrieved


def test_find_handler_id_by_function(handler_registry):
    """Test that find_handler_id_by_function correctly finds handler IDs by function reference"""
    print_message_handler = PrintMessageHandler()
    handler_func = print_message_handler.handle_messageAsync

    # Test finding handler that hasn't been registered yet
    handler_id = handler_registry.find_handler_id_by_function(handler_func)
    assert handler_id is None

    # Register the handler
    registered_id = handler_registry.register_handler(handler_func)

    # Test finding the registered handler
    found_id = handler_registry.find_handler_id_by_function(handler_func)
    assert found_id == registered_id
    assert found_id is not None

    # Test with a different function
    async def another_handler(topic: str, payload: str):
        pass

    # Should not find unregistered function
    not_found_id = handler_registry.find_handler_id_by_function(another_handler)
    assert not_found_id is None

    # Register the second handler
    another_registered_id = handler_registry.register_handler(another_handler)

    # Should find both handlers correctly
    first_found = handler_registry.find_handler_id_by_function(handler_func)
    second_found = handler_registry.find_handler_id_by_function(another_handler)

    assert first_found == registered_id
    assert second_found == another_registered_id
    assert first_found != second_found

    # Test with None (edge case)
    none_result = handler_registry.find_handler_id_by_function(None)
    assert none_result is None


def test_register_handler_invalid_signature_raises_exception(handler_registry):
    """Test that registering a handler with invalid signature raises InvalidHandlerError"""

    # Test function with 0 parameters
    async def handler_no_params():
        pass

    with pytest.raises(InvalidHandlerError) as exc_info:
        handler_registry.register_handler(handler_no_params)

    # # Verify exception details
    # error = exc_info.value
    # assert error.message == "Handler function must be callable"
    # assert error.reason_code == "invalid_handler"
    # assert "handler_error_details" in error.error_details
    #
    # error_details = error.error_details["handler_error_details"]
    # assert error_details["function_name"] == "handler_no_params"
    # assert error_details["expected_params"] == 2
    # assert error_details["actual_params"] == 0
    # assert "signature" in error_details

    # Test function with 1 parameter
    async def handler_one_param(topic: str):
        pass

    with pytest.raises(InvalidHandlerError) as exc_info:
        handler_registry.register_handler(handler_one_param)

    error = exc_info.value
    error_details = error.details["handler_error_details"]
    assert error_details["actual_params"] == 1

    # Test function with 3 parameters
    async def handler_three_params(topic: str, payload: str, extra: str):
        pass

    with pytest.raises(InvalidHandlerError) as exc_info:
        handler_registry.register_handler(handler_three_params)

    error = exc_info.value
    error_details = error.details["handler_error_details"]
    assert error_details["actual_params"] == 3


def test_INTEGRATION_Register_and_Retrieve_Handler(handler_registry):
    """Integration test for registering and retrieving a handler"""
    print_message_handler = PrintMessageHandler()
    handler_func = print_message_handler.handle_messageAsync

    # Register the handler
    handler_id = handler_registry.register_handler(handler_func)

    # Retrieve the handler by ID
    retrieved_handler = handler_registry.get_handler(handler_id)

    # Verify that the retrieved handler matches the original
    assert retrieved_handler == handler_func


def test_INTEGRATION_Register_Multiple_and_Retrieve_Exact_Handler(handler_registry):
    """Integration test for registering multiple handlers and retrieving exact ones"""

    # Create multiple different handlers
    print_message_handler = PrintMessageHandler()
    handler_func1 = print_message_handler.handle_messageAsync

    async def custom_handler_2(topic: str, payload: str):
        print(f"Custom handler 2: {topic} - {payload}")

    async def custom_handler_3(topic: str, payload: str):
        print(f"Custom handler 3: {topic} - {payload}")

    # Register all handlers
    handler_id1 = handler_registry.register_handler(handler_func1)
    handler_id2 = handler_registry.register_handler(custom_handler_2)
    handler_id3 = handler_registry.register_handler(custom_handler_3)

    # Verify all IDs are unique
    assert handler_id1 != handler_id2
    assert handler_id2 != handler_id3
    assert handler_id1 != handler_id3

    # Retrieve each handler by its specific ID
    retrieved_handler1 = handler_registry.get_handler(handler_id1)
    retrieved_handler2 = handler_registry.get_handler(handler_id2)
    retrieved_handler3 = handler_registry.get_handler(handler_id3)

    # Verify each retrieved handler matches its original
    assert retrieved_handler1 == handler_func1
    assert retrieved_handler2 == custom_handler_2
    assert retrieved_handler3 == custom_handler_3

    # Verify handlers are distinct from each other
    assert retrieved_handler1 != retrieved_handler2
    assert retrieved_handler2 != retrieved_handler3
    assert retrieved_handler1 != retrieved_handler3

    # Test finding handlers by function reference
    found_id1 = handler_registry.find_handler_id_by_function(handler_func1)
    found_id2 = handler_registry.find_handler_id_by_function(custom_handler_2)
    found_id3 = handler_registry.find_handler_id_by_function(custom_handler_3)

    # Verify found IDs match the original registration IDs
    assert found_id1 == handler_id1
    assert found_id2 == handler_id2
    assert found_id3 == handler_id3
