from typing import Optional
import inspect

from mqtt_connector_lib.handlers import PrintMessageHandler
from mqtt_connector_lib.interfaces import IHandlerRegistry,HandlerFunc
from mqtt_connector_lib.exceptions import HandlerNotFoundError,HandlerRegistryError,InvalidHandlerError


import logging
from mqtt_connector_lib import constants


adapter_context = {'prefix': constants.HANDLER_REGISTRY_PREFIX}
# logger = logging.LoggerAdapter(logging.getLogger(constants.SERVICE_NAME), adapter_context)
logger = logging.getLogger(constants.SERVICE_NAME)
logger = logging.LoggerAdapter(logger, adapter_context)


class HandlerRegistry(IHandlerRegistry):
    """Concrete implementation of handler registry"""
    def __init__(self):
        self._handler_registry: dict[str, HandlerFunc] = {}
        # self._default_handler = default_handler
        # # Register default handler
        # self.register_handler(default_handler)
        self.error_details = {
                "handler_error_details": {
                    # "function_name": getattr(handler_func, '__name__', 'unknown'),
                    # "expected_params": 2,
                    # "actual_params": len(sig.parameters),
                    # "signature": str(sig)
                }
            }

    def get_handler_id(self, func: HandlerFunc) -> str:
        """Generate a unique ID for a handler function"""
        return self.find_handler_id_by_function(func)
        # # ⚠️ At runtime this is fine, but type checkers may warn since Callable doesn't declare __module__/__qualname__
        # try:
        #     return f"{func.__module__}.{func.__qualname__}"
        # except AttributeError:
        #     # Fallback for edge cases where __module__ or __qualname__ might not exist
        #     return f"handler_{id(func)}"

    def _generate_handler_id(self, func: HandlerFunc) -> str:
        """Generate a unique ID for a handler function"""
        # ⚠️ At runtime this is fine, but type checkers may warn since Callable doesn't declare __module__/__qualname__
        try:
            return f"{func.__module__}.{func.__qualname__}"
        except AttributeError:
            # Fallback for edge cases where __module__ or __qualname__ might not exist
            return f"handler_{id(func)}"


    def register_handler(self, handler_func: HandlerFunc, user_friendly_name:str=None) -> str:
        """Register a handler function and return its ID"""
        # if not callable(handler_func):
        #     # raise ValueError(f"Handler function must be callable, got {type(handler_func)}")
        #     error_details = {"handler_error_details": {
        #         "provided_type": type(handler_func).__name__,
        #         "provided_value": str(handler_func),
        #         "function_name": getattr(handler_func, '__name__', 'unknown'),
        #         "function_module": getattr(handler_func, '__module__', 'unknown'),
        #         "is_callable": callable(handler_func),
        #         "expected_type": "Callable[[str, str], Awaitable[None]]"
        #     }}
        #     raise InvalidHandlerError(message="Handler function must be callable",
        #                               reason_code= "invalid_handler",
        #                               **error_details)

        # # Validate function signature
        # sig = inspect.signature(handler_func)
        # if len(sig.parameters) != 2:
        #     error_details = {
        #         "handler_error_details": {
        #             "function_name": getattr(handler_func, '__name__', 'unknown'),
        #             "expected_params": 2,
        #             "actual_params": len(sig.parameters),
        #             "signature": str(sig)
        #         }
        #     }
        #     handler_error = InvalidHandlerError(message="Handler function must be callable",
        #                               reason_code="invalid_handler",
        #                               **error_details)
        #     logger.error(f"{handler_error}")
        #     raise handler_error

        # handler_id = self.get_handler_id(handler_func)
        if user_friendly_name is not None and user_friendly_name.strip() == "":
            self.error_details['handler_error_details']['provided_name'] = user_friendly_name
            self.error_details['handler_error_details']['profided_handler_function'] = handler_func
            self.error_details['handler_error_details']['HOW_TO_FIX'] = 'While registering the handler function, provide a non-empty string as user_friendly_name or None to auto-generate the handler ID.'
            raise InvalidHandlerError(
            message="Handler name cannot be an empty string", reason_code="invalid_handler_name", **self.error_details)

        # Usr_friendly_name provided, check for duplicates
        if user_friendly_name in self._handler_registry:
            stored_handler_function = self.get_handler(user_friendly_name)
            if stored_handler_function == handler_func:     # registering the same handler once again with the same name
                logger.warning(
                    f"Handler function '{getattr(handler_func, '__name__', 'unknown')}' is already registered with ID '{user_friendly_name}'. "
                    f"Skipping duplicate registration and returning existing ID.")
                return user_friendly_name
            self.error_details['handler_error_details']['existing_handler_id'] = user_friendly_name
            self.error_details['handler_error_details']['provided_handler_function'] = handler_func
            self.error_details['handler_error_details'][
                'HOW_TO_FIX'] = 'Choose a different user_friendly_name or unregister the existing handler first.'
            raise InvalidHandlerError(
                message=f"Handler with ID '{user_friendly_name}' is already registered",
                reason_code="duplicate_handler_id",
                **self.error_details
            )

        existing_handler_id = self.find_handler_id_by_function(handler_func)
        if existing_handler_id:
            if user_friendly_name:
                self.error_details['handler_error_details']['existing_handler_id'] = existing_handler_id
                self.error_details['handler_error_details']['provided_handler_function'] = handler_func
                self.error_details['handler_error_details']['requested_name'] = user_friendly_name
                self.error_details['handler_error_details'][
                    'HOW_TO_FIX'] = 'The handler function is already registered with different name {existing_handler_id}. Unregister it first or use the existing handler ID.'
                raise InvalidHandlerError(
                    message=f"Handler function '{getattr(handler_func, '__name__', 'unknown')}' is already registered with ID '{existing_handler_id}'",
                    reason_code="duplicate_handler_function", **self.error_details)
            else:
                logger.warning(
                    f"Handler function '{getattr(handler_func, 'name', 'unknown')}' is already registered with ID '{existing_handler_id}'. "
                    f"Skipping duplicate registration and returning existing ID.")
                return existing_handler_id



        handler_id : str = user_friendly_name if user_friendly_name else self._generate_handler_id(handler_func)

        # if handler_id in self._handler_registry:
        #     logger.warning(f"Handler : '{handler_id}' is already registered, overwriting it.")
        # self._handler_registry.get()



        self._handler_registry[handler_id] = handler_func
        logger.info(f"Handler  '{getattr(handler_func, 'name', 'unknown')}' registered with handler id:  '{handler_id}'")
        return handler_id

    def unregister_handler(self, handler_id: str) -> bool:
        """Remove a handler from the registry"""
        if handler_id in self._handler_registry:
            del self._handler_registry[handler_id]
            logger.info(f"Unregistered handler '{handler_id}'")
            return True
        return False

    def get_handler(self, handler_id: str) -> HandlerFunc:
        """Get a handler function by its ID"""
        return self._handler_registry.get(handler_id, None)
        # handler = self._handler_registry.get(handler_id)
        # if handler is None:
        #     raise HandlerNotFoundError(f"Handler with ID '{handler_id}' not found.")
        # return handler
        # return self._handler_registry.get(handler_id)


    # def find_handler_id_by_function(self, func: HandlerFunc) -> Optional[str]:
    #     """Find handler ID by comparing function objects"""
    #     for handler_id, registered_func in self._handler_registry.items():
    #         if registered_func == func:
    #             return handler_id
    #     return None
    def find_handler_id_by_function(self, func: HandlerFunc) -> str | None:
        """
        Find handler ID by comparing function objects.

        Raises:
            HandlerNotFoundError: If the handler function is not found in the registry.
        """
        for handler_id, registered_func in self._handler_registry.items():
            if registered_func == func:
                return handler_id
        return None # raise HandlerNotFoundError(f"Handler function '{getattr(func, '__name__', 'unknown')}' not found in registry.")

