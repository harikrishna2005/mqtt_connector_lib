from typing import Callable, Awaitable,TypeAlias, Optional
from abc import ABC, abstractmethod

HandlerFunc: TypeAlias = Callable[[str, str], Awaitable[None]]


class IMessageHandler(ABC):
    @abstractmethod
    async def handle_messageAsync(self, topic: str, payload: str): pass


class IHandlerRegistry(ABC):
    """Interface for a handler registry"""
    # @abstractmethod
    # def get_handler_id(self, func: HandlerFunc) -> str:
    #     """Generate a unique ID for a handler function"""
    #     raise NotImplementedError

    @abstractmethod
    def register_handler(self, handler_func: HandlerFunc) -> str:
        """Register a handler function and return its ID"""
        raise NotImplementedError

    # @abstractmethod
    # def get_handler(self, handler_id: str) -> Optional[HandlerFunc]:
    #     """Get a handler function by its ID"""
    #     raise NotImplementedError
    #
    # @abstractmethod
    # def unregister_handler(self, handler_id: str) -> bool:
    #     """Remove a handler from the registry"""
    #     raise NotImplementedError
    #
    # @abstractmethod
    # def get_default_handler(self) -> HandlerFunc:
    #     """Return the default handler"""
    #     raise NotImplementedError
    #
    # @abstractmethod
    # def find_handler_id_by_function(self, func: HandlerFunc) -> Optional[str]:
    #     """Find handler ID by comparing function objects"""
    #     raise NotImplementedError