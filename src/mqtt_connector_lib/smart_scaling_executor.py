# -----------------------------------------------------------
# Future Improvements:
# -----------------------------------------------------------
#
# Add optional callbacks for warnings (webhook/metrics/alerts).
#
# Emit Prometheus metrics for queue_size, worker_count, and cpu_ewma.
#
# Run a quick unit test or a small simulation script to demonstrate scaling behavior.


import asyncio
import logging
import psutil
import os
import inspect
from typing import Any, Tuple, List
from mqtt_connector_lib import constants
from mqtt_connector_lib.interfaces import HandlerFunc

adapter_context = {'prefix': constants.ON_MESSAGE_PREFIX}
logger = logging.getLogger(constants.SERVICE_NAME)
logger = logging.LoggerAdapter(logger, adapter_context)


class SmartScalingExecutor:
    """
    Advanced worker executor with the following improvements and features:
      - Minimum workers: configurable (default 3)
      - Monotonically increasing worker IDs to avoid ID reuse/confusion
      - Dynamic scale-up/down using:
          * queue delta (Δ queue)
          * CPU EWMA smoothing
          * 1-minute load average (unix-only, safe on Windows)
          * persistent load detection (CPU EWMA + queue usage)
      - Queue usage warning system (70%, 85%, 95%)
      - Safe handling of sync or async handler functions
      - Graceful shutdown with timeout to avoid indefinite hangs
      - Portable: os.getloadavg() guarded for non-Unix systems
    """

    # Queue usage thresholds (percent)
    WARN_70 = 70
    WARN_85 = 85
    WARN_95 = 95

    def __init__(
        self,
        min_workers: int = 3,
        max_workers: int = 20,
        ewma_alpha: float = 0.2,
        queue_check_interval: float = 2.0,
        queue_size: int = 2000,
        shutdown_wait_seconds: float = 10.0,
    ):
        self.min_workers = max(1, min_workers)
        self.max_workers = max(self.min_workers, max_workers)
        self.ewma_alpha = max(0.0, min(1.0, ewma_alpha))
        self.queue_check_interval = max(0.1, queue_check_interval)
        self.shutdown_wait_seconds = shutdown_wait_seconds

        # Worker storage: list of (worker_id, asyncio.Task)
        self.workers: List[Tuple[int, asyncio.Task]] = []

        # Monotonic worker id counter to avoid reusing IDs.
        self._next_worker_id = 0

        # Async queue for incoming handlers
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=queue_size)

        # State flags
        self.shutdown_flag = False

        # Metrics / internals
        self._last_queue_size = 0
        # Start cpu_ewma at 0.0; first cpu_percent call will measure since interpreter start
        self._cpu_ewma = 0.0
        self._low_queue_intervals = 0
        self._previous_usage = 0.0

        # Background autoscaler task handle (optional)
        self._autoscaler_task: asyncio.Task | None = None

    # -------------------------
    # Public API
    # -------------------------
    async def start(self):
        """Start the executor with the configured minimum workers and start autoscaler."""
        logger.info(f"Starting SmartScalingExecutor with min_workers={self.min_workers}, max_workers={self.max_workers}")
        for _ in range(self.min_workers):
            self._start_worker()

        # Start autoscaler as background task and keep reference to cancel on stop
        if self._autoscaler_task is None or self._autoscaler_task.done():
            self._autoscaler_task = asyncio.create_task(self._auto_scaler(), name="smartscaler")

    async def stop(self):
        """Stop the executor gracefully, cancel workers and wait with timeout."""
        logger.info("Stopping SmartScalingExecutor...")
        self.shutdown_flag = True

        # Cancel autoscaler background task first (so it stops spawning)
        if self._autoscaler_task is not None and not self._autoscaler_task.done():
            self._autoscaler_task.cancel()
            try:
                await asyncio.wait_for(self._autoscaler_task, timeout=1.0)
            except asyncio.CancelledError:
                pass
            except asyncio.TimeoutError:
                logger.debug("Autoscaler did not stop quickly; continuing shutdown.")

        # Cancel all workers
        for wid, task in list(self.workers):
            if not task.done():
                task.cancel()

        # Wait for workers to finish with timeout
        if self.workers:
            tasks = [t for _, t in self.workers]
            try:
                await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=self.shutdown_wait_seconds)
            except asyncio.TimeoutError:
                logger.warning("Timeout while waiting for workers to stop; some workers may be stuck.")

        # Clear worker list
        self.workers.clear()
        logger.info("SmartScalingExecutor stopped.")

    def submit(self, topic: str, payload: Any, handler: HandlerFunc) -> bool:
        """
        Submit (topic,payload,handler) tuple to the queue.
        Returns True if queued successfully, False if queue is full (drop/overflow policy).
        """
        try:
            self.queue.put_nowait((topic, payload, handler))
            return True
        except asyncio.QueueFull:
            logger.error("Queue FULL → dropping incoming message.")
            return False

    def get_queue_size(self) -> int:
        return self.queue.qsize()

    # -------------------------
    # Internal helpers
    # -------------------------
    def _start_worker(self) -> int:
        """Start a worker with a unique monotonically increasing id and return the id."""
        worker_id = self._next_worker_id
        self._next_worker_id += 1

        task = asyncio.create_task(self._worker_loop(worker_id), name=f"worker-{worker_id}")
        self.workers.append((worker_id, task))
        logger.info(f"Started worker-{worker_id}. Total workers: {len(self.workers)}")
        return worker_id

    async def _worker_loop(self, worker_id: int):
        """Worker loop which supports both async and sync handlers."""
        name = f"worker-{worker_id}"
        try:
            while not self.shutdown_flag:
                try:
                    topic, payload, handler = await self.queue.get()

                    try:
                        # Support both coroutine (async) and normal functions
                        result = handler(topic, payload)
                        if inspect.isawaitable(result):
                            await result
                        # If result is not awaitable, it means handler was synchronous and already executed.
                    except Exception as e:
                        logger.error(f"Handler error in {name}: {e}")
                    finally:
                        # Must call task_done() regardless of handler success/failure to keep queue consistent
                        try:
                            self.queue.task_done()
                        except Exception:
                            # Defensive: task_done can raise if task wasn't gotten properly; ignore to keep loop alive
                            pass

                except asyncio.CancelledError:
                    logger.info(f"{name} cancelled gracefully.")
                    break
                except Exception as e:
                    # Catch-all to avoid worker dying unexpectedly
                    logger.exception(f"Unexpected exception in {name}: {e}")
                    # small sleep to avoid tight error loop
                    await asyncio.sleep(0.1)
        finally:
            logger.info(f"{name} exiting.")

    # -------------------------
    # Autoscaler logic
    # -------------------------
    async def _auto_scaler(self):
        """Autoscaler background loop that periodically checks metrics and scales workers."""
        logger.debug("Autoscaler started.")
        # Ensure psutil has an initial baseline reading
        try:
            _ = psutil.cpu_percent(interval=None)
        except Exception:
            # If psutil misbehaves, set to 0 baseline
            pass

        while not self.shutdown_flag:
            await asyncio.sleep(self.queue_check_interval)

            qsize = self.queue.qsize()
            maxsize = self.queue.maxsize
            usage = (qsize / maxsize) * 100 if maxsize > 0 else 0.0

            # System load average (unix-only). Be safe on Windows.
            load_avg = 0.0
            try:
                load_avg = os.getloadavg()[0]
            except Exception:
                load_avg = 0.0

            # Sample CPU now (measures since last call)
            try:
                cpu_now = psutil.cpu_percent(interval=None)
            except Exception:
                cpu_now = 0.0

            # EWMA smoothing
            self._cpu_ewma = (self.ewma_alpha * cpu_now) + ((1.0 - self.ewma_alpha) * self._cpu_ewma)

            # queue delta
            delta_q = qsize - self._last_queue_size
            self._last_queue_size = qsize

            # Debug log
            logger.debug(
                f"[Scaler] q={qsize}, Δq={delta_q}, usage={usage:.1f}%, load={load_avg:.2f}, CPU_EWMA={self._cpu_ewma:.1f}%, workers={len(self.workers)}"
            )

            # -------------------------
            # Queue usage warnings (threshold crossing only)
            # -------------------------
            prev = self._previous_usage
            if usage >= self.WARN_95 and prev < self.WARN_95:
                logger.error(f"[CRITICAL] Queue usage {usage:.1f}% ({qsize}/{maxsize})")
            elif usage >= self.WARN_85 and prev < self.WARN_85:
                logger.warning(f"[HIGH] Queue usage {usage:.1f}% ({qsize}/{maxsize})")
            elif usage >= self.WARN_70 and prev < self.WARN_70:
                logger.warning(f"[WARN] Queue usage {usage:.1f}% ({qsize}/{maxsize})")
            elif usage < self.WARN_70 and prev >= self.WARN_70:
                logger.info(f"[RECOVERED] Queue usage dropped to {usage:.1f}% ({qsize}/{maxsize})")
            self._previous_usage = usage

            # -------------------------
            # IMMEDIATE handling for FULL queue: log + drop (submit() already does this).
            # We intentionally DO NOT auto-scale purely because of 'full' since full indicates
            # the system or configured max_workers has already been reached or handlers too slow.
            # -------------------------
            if qsize >= maxsize:
                logger.error("Queue is FULL and messages will be dropped (submit() returns False).")
                # we've already dropped in submit; continue to next iteration
                continue

            # -------------------------
            # Scale-up triggers:
            #  - Spike detection: delta_q > threshold
            #  - OR persistent condition: CPU_EWMA >= 60% AND usage >= 70%
            # Additionally require that CPU_EWMA < 70% and load_avg < 1.2 to avoid oversubscription
            # -------------------------
            spike_threshold = 20
            persistent_cpu_threshold = 60.0
            persistent_usage_threshold = self.WARN_70

            spike_detected = (delta_q > spike_threshold)
            persistent_detected = (self._cpu_ewma >= persistent_cpu_threshold and usage >= persistent_usage_threshold)

            scale_up_trigger = spike_detected or persistent_detected

            if (
                scale_up_trigger
                and self._cpu_ewma < 70.0
                and load_avg < 1.2
                and len(self.workers) < self.max_workers
            ):
                self._start_worker()
                logger.info("SCALE UP: added one worker (triggered by spike or persistent load).")
                # continue to next iteration to avoid immediate scale-down logic
                continue

            # -------------------------
            # Scale-down logic:
            # If queue small for multiple intervals and CPU_EWMA low, remove one worker (but keep min_workers)
            # -------------------------
            if qsize < 5:
                self._low_queue_intervals += 1
            else:
                self._low_queue_intervals = 0

            if (
                self._low_queue_intervals >= 5
                and self._cpu_ewma < 40.0
                and len(self.workers) > self.min_workers
            ):
                # Remove the last worker in the list (LIFO), cancel and remove from bookkeeping
                worker_id, task = self.workers.pop()
                if not task.done():
                    task.cancel()
                logger.info(f"SCALE DOWN: removed worker-{worker_id}. Remaining workers={len(self.workers)}")
                # reset counter
                self._low_queue_intervals = 0

        logger.debug("Autoscaler exiting.")
