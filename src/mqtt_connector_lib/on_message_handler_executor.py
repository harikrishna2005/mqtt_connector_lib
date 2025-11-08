# src/mqtt_connector_lib/on_message_handler_executor.py
import asyncio
import logging
from typing import Any
from mqtt_connector_lib import constants
from mqtt_connector_lib.interfaces import HandlerFunc

adapter_context = {'prefix': constants.ON_MESSAGE_PREFIX}
# logger = logging.LoggerAdapter(logging.getLogger(constants.SERVICE_NAME), adapter_context)
logger = logging.getLogger(constants.SERVICE_NAME)
logger = logging.LoggerAdapter(logger, adapter_context)


# ===========  Only ASYNC handler support =============
class OnMessageHandlerExecutor:
    """Executes handlers from _on_message callback using async worker pool pattern"""

    def __init__(self, max_workers: int = 5, queue_size: int = 1000, thread_workers: int = 3):
        self._on_message_handler_queue = asyncio.Queue(maxsize=queue_size)
        self._workers = []
        self._max_workers = max_workers
        self._shutdown = False
        # self._thread_pool = ThreadPoolExecutor(
        #     max_workers=thread_workers,
        #     thread_name_prefix="on-message-handler"
        # )

    async def start(self):
        """Start _on_message handler execution workers"""
        for i in range(self._max_workers):
            worker = asyncio.create_task(self._on_message_worker(f"on-message-worker-{i}"))
            self._workers.append(worker)
        logger.info(f"Started {self._max_workers} _on_message handler execution workers")

    async def stop(self):
        """Gracefully stop all _on_message workers"""
        logger.debug("Stopping _on_message handler executor...")
        self._shutdown = True

        # Cancel all worker tasks
        for worker in self._workers:
            if not worker.done():
                worker.cancel()

        # Wait for all workers to finish with a timeout
        if self._workers:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._workers, return_exceptions=True),
                    timeout=5.0
                )
            except asyncio.TimeoutError:
                logger.warning("Timeout waiting for workers to stop, forcing shutdown")

        self._workers.clear()
        logger.info("Stopped all _on_message handler execution workers")

    def execute_on_message_handler(self, topic: str, payload: Any, handler: HandlerFunc) -> bool:
        """Queue handler from _on_message for execution. Returns False if queue is full."""
        try:
            self._on_message_handler_queue.put_nowait((topic, payload, handler))
            return True
        except asyncio.QueueFull:
            logger.warning(f"_on_message handler queue full, dropping execution for topic: {topic}")
            return False

    # async def _on_message_worker(self, worker_name: str):
    #     """Worker that executes handlers from _on_message queue"""
    #     while not self._shutdown:
    #         try:
    #             topic, payload, handler = await asyncio.wait_for(
    #                 self._on_message_handler_queue.get(), timeout=1.0
    #             )
    #
    #             # await self._execute_on_message_handler(topic, payload, handler)
    #             await handler(topic, payload)
    #             self._on_message_handler_queue.task_done()
    #
    #         except asyncio.TimeoutError:
    #             continue
    #         except Exception as e:
    #             logger.error(f"_on_message handler worker {worker_name} error: {e}")

    async def _on_message_worker(self, worker_name: str):
        """Worker that executes handlers from _on_message queue"""
        while not self._shutdown:
            try:
                topic, payload, handler = await asyncio.wait_for(
                    self._on_message_handler_queue.get(), timeout=1.0
                )

                try:
                    await handler(topic, payload)
                except Exception as handler_error:
                    logger.error(f"Handler execution error for topic {topic}: {handler_error}")
                finally:
                    # Always mark task as done, even if handler fails
                    self._on_message_handler_queue.task_done()

            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                # Handle graceful shutdown when task is cancelled
                logger.debug(f"Worker {worker_name} cancelled during shutdown")
                break
            except Exception as e:
                logger.error(f"_on_message handler worker {worker_name} error: {e}")


    # async def _execute_on_message_handler(self, topic: str, payload: Any, handler: Callable):
    #     """Execute handler from _on_message callback (handles both sync/async)"""
    #     try:
    #         if asyncio.iscoroutinefunction(handler):
    #             # Async handler - await it properly
    #             await handler(topic, payload)
    #         else:
    #             # Sync handler - run in thread pool
    #             loop = asyncio.get_event_loop()
    #             await loop.run_in_executor(self._thread_pool, handler, topic, payload)
    #     except Exception as e:
    #         logger.error(f"_on_message handler execution error for topic {topic}: {e}")

    def get_on_message_queue_size(self) -> int:
        """Get current _on_message handler queue size"""
        return self._on_message_handler_queue.qsize()

# ===========  with SYNC and ASYNC handler support =============
# class OnMessageHandlerExecutor:
#     """Executes handlers from _on_message callback using async worker pool pattern"""
#
#     def __init__(self, max_workers: int = 5, queue_size: int = 1000, thread_workers: int = 3):
#         self._on_message_handler_queue = asyncio.Queue(maxsize=queue_size)
#         self._workers = []
#         self._max_workers = max_workers
#         self._shutdown = False
#         self._thread_pool = ThreadPoolExecutor(
#             max_workers=thread_workers,
#             thread_name_prefix="on-message-handler"
#         )
#
#     async def start(self):
#         """Start _on_message handler execution workers"""
#         for i in range(self._max_workers):
#             worker = asyncio.create_task(self._on_message_worker(f"on-message-worker-{i}"))
#             self._workers.append(worker)
#         logger.info(f"Started {self._max_workers} _on_message handler execution workers")
#
#     async def stop(self):
#         """Gracefully stop all _on_message workers"""
#         self._shutdown = True
#         self._thread_pool.shutdown(wait=True)
#
#         if self._workers:
#             await asyncio.gather(*self._workers, return_exceptions=True)
#
#         logger.info("Stopped all _on_message handler execution workers")
#
#     def execute_on_message_handler(self, topic: str, payload: Any, handler: Callable) -> bool:
#         """Queue handler from _on_message for execution. Returns False if queue is full."""
#         try:
#             self._on_message_handler_queue.put_nowait((topic, payload, handler))
#             return True
#         except asyncio.QueueFull:
#             logger.warning(f"_on_message handler queue full, dropping execution for topic: {topic}")
#             return False
#
#     async def _on_message_worker(self, worker_name: str):
#         """Worker that executes handlers from _on_message queue"""
#         while not self._shutdown:
#             try:
#                 topic, payload, handler = await asyncio.wait_for(
#                     self._on_message_handler_queue.get(), timeout=1.0
#                 )
#
#                 await self._execute_on_message_handler(topic, payload, handler)
#                 self._on_message_handler_queue.task_done()
#
#             except asyncio.TimeoutError:
#                 continue
#             except Exception as e:
#                 logger.error(f"_on_message handler worker {worker_name} error: {e}")
#
#     async def _execute_on_message_handler(self, topic: str, payload: Any, handler: Callable):
#         """Execute handler from _on_message callback (handles both sync/async)"""
#         try:
#             if asyncio.iscoroutinefunction(handler):
#                 # Async handler - await it properly
#                 await handler(topic, payload)
#             else:
#                 # Sync handler - run in thread pool
#                 loop = asyncio.get_event_loop()
#                 await loop.run_in_executor(self._thread_pool, handler, topic, payload)
#         except Exception as e:
#             logger.error(f"_on_message handler execution error for topic {topic}: {e}")
#
#     def get_on_message_queue_size(self) -> int:
#         """Get current _on_message handler queue size"""
#         return self._on_message_handler_queue.qsize()
#







