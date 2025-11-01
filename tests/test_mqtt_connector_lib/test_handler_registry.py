import pytest_asyncio
import pytest
import logging

from mqtt_connector_lib.exceptions import InvalidHandlerError, HandlerNotFoundError

logging.disable(logging.CRITICAL)


from mqtt_connector_lib.handler_registry import HandlerRegistry
from mqtt_connector_lib.handlers import PrintMessageHandler
@pytest_asyncio.fixture
def handler_registry():
    return HandlerRegistry()


# def test_get_handler_id(handler_registry):
#     """Test that get_handler_id generates consistent IDs for handler functions"""
#     print_message_handler = PrintMessageHandler()
#     handler_func = print_message_handler.handle_messageAsync
#
#     # Get handler ID
#     handler_id = handler_registry.get_handler_id(handler_func)
#
#     # Handler ID should be a non-empty string
#     assert isinstance(handler_id, str)
#     assert len(handler_id) > 0


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
    # with pytest.raises(HandlerNotFoundError):
    #     handler_registry.get_handler("non_existent_id")
    non_existant_handler = handler_registry.get_handler("non_existent_id")
    assert non_existant_handler is None

    # Test with empty string
    # with pytest.raises(HandlerNotFoundError):
    empty_string_handler = handler_registry.get_handler("")
    assert non_existant_handler is None

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
    # with pytest.raises(HandlerNotFoundError):
    #     handler_registry.find_handler_id_by_function(handler_func)

    result = handler_registry.find_handler_id_by_function(handler_func)
    assert result is None

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
    # with pytest.raises(HandlerNotFoundError):
    #     handler_registry.find_handler_id_by_function(another_handler)
    result = handler_registry.find_handler_id_by_function(another_handler)
    assert result is None

    # Register the second handler
    another_registered_id = handler_registry.register_handler(another_handler)

    # Should find both handlers correctly
    first_found = handler_registry.find_handler_id_by_function(handler_func)
    second_found = handler_registry.find_handler_id_by_function(another_handler)

    assert first_found == registered_id
    assert second_found == another_registered_id
    assert first_found != second_found

    # Test with None (edge case)
    # with pytest.raises(HandlerNotFoundError):
    #     handler_registry.find_handler_id_by_function(None)
    result = handler_registry.find_handler_id_by_function(None)
    assert result is None


#
# def test_register_handler_invalid_signature_raises_exception(handler_registry):
#     """Test that registering a handler with invalid signature raises InvalidHandlerError"""
#
#     # Test function with 0 parameters
#     async def handler_no_params():
#         pass
#
#     with pytest.raises(InvalidHandlerError) as exc_info:
#         handler_registry.register_handler(handler_no_params)
#
#     # # Verify exception details
#     # error = exc_info.value
#     # assert error.message == "Handler function must be callable"
#     # assert error.reason_code == "invalid_handler"
#     # assert "handler_error_details" in error.error_details
#     #
#     # error_details = error.error_details["handler_error_details"]
#     # assert error_details["function_name"] == "handler_no_params"
#     # assert error_details["expected_params"] == 2
#     # assert error_details["actual_params"] == 0
#     # assert "signature" in error_details
#
#     # Test function with 1 parameter
#     async def handler_one_param(topic: str):
#         pass
#
#     with pytest.raises(InvalidHandlerError) as exc_info:
#         handler_registry.register_handler(handler_one_param)
#
#     error = exc_info.value
#     error_details = error.details["handler_error_details"]
#     assert error_details["actual_params"] == 1
#
#     # Test function with 3 parameters
#     async def handler_three_params(topic: str, payload: str, extra: str):
#         pass
#
#     with pytest.raises(InvalidHandlerError) as exc_info:
#         handler_registry.register_handler(handler_three_params)
#
#     error = exc_info.value
#     error_details = error.details["handler_error_details"]
#     assert error_details["actual_params"] == 3


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

    async def custom_handler_4(topic: str, payload: str):
        print(f"Custom handler 4: {topic} - {payload}")

    async def custom_handler_5(topic: str, payload: str):
        print(f"Custom handler 5: {topic} - {payload}")

    # Register all handlers
    handler_id1 = handler_registry.register_handler(handler_func1)
    handler_id2 = handler_registry.register_handler(custom_handler_2)
    handler_id3 = handler_registry.register_handler(custom_handler_3)
    handler_id4 = handler_registry.register_handler(custom_handler_4,"myhandler")  # Register first handler again
    handler_id5 = handler_registry.register_handler(custom_handler_5)

    # Verify all IDs are unique
    assert handler_id1 != handler_id2
    assert handler_id2 != handler_id3
    assert handler_id1 != handler_id3
    assert  handler_id4 != handler_id1

    # Retrieve each handler by its specific ID
    retrieved_handler1 = handler_registry.get_handler(handler_id1)
    retrieved_handler2 = handler_registry.get_handler(handler_id2)
    retrieved_handler3 = handler_registry.get_handler(handler_id3)
    retrieved_handler4 = handler_registry.get_handler(handler_id4)

    # Verify each retrieved handler matches its original
    assert retrieved_handler1 == handler_func1
    assert retrieved_handler2 == custom_handler_2
    assert retrieved_handler3 == custom_handler_3
    assert retrieved_handler4 == custom_handler_4

    # Verify handlers are distinct from each other
    assert retrieved_handler1 != retrieved_handler2
    assert retrieved_handler2 != retrieved_handler3
    assert retrieved_handler1 != retrieved_handler3
    assert retrieved_handler4 != retrieved_handler1

    # Test finding handlers by function reference
    found_id1 = handler_registry.find_handler_id_by_function(handler_func1)
    found_id2 = handler_registry.find_handler_id_by_function(custom_handler_2)
    found_id3 = handler_registry.find_handler_id_by_function(custom_handler_3)
    found_id4 = handler_registry.find_handler_id_by_function(handler_func1)

    # Verify found IDs match the original registration IDs
    assert found_id1 == handler_id1
    assert found_id2 == handler_id2
    assert found_id3 == handler_id3
    # assert found_id4 == handler_id4


# Dummy handlers for testing
async def sample_handler_one(topic: str, payload: str):
    pass


async def sample_handler_two(topic: str, payload: str):
    pass


class TestRegisterHandlerScenarios:
    """
    Contains integration tests for various scenarios of the register_handler method.
    """

    @pytest.fixture
    def handler_registry(self) -> HandlerRegistry:
        """Provides a clean HandlerRegistry instance for each test."""
        return HandlerRegistry()

    def test_register_with_friendly_name_then_without_keeps_friendly_name(self, handler_registry: HandlerRegistry):
        """
        Scenario 1: If a handler is registered with a user-friendly name and then
        re-registered without one, the original friendly name should be retained.
        """
        # Arrange
        friendly_name = "my_special_handler"

        # Act
        # 1. Register with a user-friendly name
        handler_registry.register_handler(sample_handler_one, user_friendly_name=friendly_name)

        # 2. Re-register the same function without a name (should be a no-op)
        handler_registry.register_handler(sample_handler_one)

        # 3. Find the ID by the function object
        found_id = handler_registry.find_handler_id_by_function(sample_handler_one)

        # Assert
        assert found_id == friendly_name

    def test_register_with_duplicate_id_raises_error(self, handler_registry: HandlerRegistry):
        """
        Scenario 2: Attempting to register a new handler with an already
        existing user-friendly name/ID should raise InvalidHandlerError.
        """
        # Arrange
        friendly_name = "duplicate_handler_name"
        # Register the first handler successfully
        handler_registry.register_handler(sample_handler_one, user_friendly_name=friendly_name)

        # Act & Assert
        # Expect an error when registering a different handler with the same name
        with pytest.raises(InvalidHandlerError, match=f"Handler with ID '{friendly_name}' is already registered"):
            handler_registry.register_handler(sample_handler_two, user_friendly_name=friendly_name)

    def test_register_without_friendly_name_generates_id(self, handler_registry: HandlerRegistry):
        """
        Scenario 3: If a handler is registered without a user-friendly name,
        a unique ID should be generated automatically.
        """
        # Arrange
        # Manually generate the expected ID to compare against
        expected_id = handler_registry._generate_handler_id(sample_handler_one)

        # Act
        # Register the handler without providing a name
        actual_id = handler_registry.register_handler(sample_handler_one)

        # Assert
        # The returned ID and the retrieved handler should match the generated ID
        assert actual_id == expected_id
        assert handler_registry.get_handler(expected_id) is sample_handler_one

    def test_register_without_name_then_with_name_raises_error(self, handler_registry: HandlerRegistry):
        """
        Bonus Scenario: Registering a handler first without a name (auto-generated ID)
        and then with a friendly name should raise an error, as a function can only have one ID.
        """
        # Arrange
        # 1. Register without a name, which generates an ID
        generated_id = handler_registry.register_handler(sample_handler_one)
        new_friendly_name = "new_friendly_name"

        # Act & Assert
        # 2. Attempt to register the same function again with a new friendly name
        with pytest.raises(InvalidHandlerError, match=f"Handler function 'sample_handler_one' is already registered with ID '{generated_id}'"):
            handler_registry.register_handler(sample_handler_one, user_friendly_name=new_friendly_name)

    def test_register_same_function_multiple_times_returns_same_id(self, handler_registry: HandlerRegistry):
        """
        Tests that registering the exact same handler function multiple times
        without a user-friendly name consistently returns the same auto-generated ID.
        """
        # Arrange
        # Act
        # 1. Register the handler for the first time
        first_id = handler_registry.register_handler(sample_handler_one)

        # 2. Register the same handler again
        second_id = handler_registry.register_handler(sample_handler_one)

        # 3. And a third time for good measure
        third_id = handler_registry.register_handler(sample_handler_one)

        # Assert
        # All returned IDs should be identical, and not None
        assert first_id is not None
        assert first_id == second_id
        assert second_id == third_id


    def test_register_same_function_with_friendly_name_multiple_times(self, handler_registry: HandlerRegistry):
        """
        Tests that registering the same handler with the same user-friendly name
        multiple times consistently returns that friendly name.
        """
        # Arrange
        friendly_name = "my_named_handler"

        # Act
        # 1. Register the handler with a friendly name
        first_id = handler_registry.register_handler(sample_handler_one, user_friendly_name=friendly_name)

        # 2. Register it again with the same name
        second_id = handler_registry.register_handler(sample_handler_one, user_friendly_name=friendly_name)

        # 3. And a third time
        third_id = handler_registry.register_handler(sample_handler_one, user_friendly_name=friendly_name)

        # Assert
        # All returned IDs should be the same as the provided friendly name
        assert first_id == friendly_name
        assert second_id == friendly_name
        assert third_id == friendly_name