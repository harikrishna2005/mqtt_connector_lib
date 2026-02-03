# -----------------------------------------------------------
# Future Improvements:
# -----------------------------------------------------------
#
# Add optional callbacks for warnings (webhook/metrics/alerts).
#
# Emit Prometheus metrics for queue_size, worker_count, and cpu_ewma.
#
# Run a quick unit test or a small simulation script to demonstrate scaling behavior.

#-----------------------------------------------------------
# Metrics call back modifications
# -----------------------------------------------------------
#
#  *******     CHANGE 1 of 2 ***********
# def __init__(..., metrics_cb=None):
#     ...
#     self.metrics_cb = metrics_cb
#
#
#  *******     CHANGE 2 of 2 ***********
# Then inside _auto_scaler() BEFORE warnings:
# if self.metrics_cb:
#     self.metrics_cb(
#         cpu=self._cpu_ewma,
#         mem=psutil.virtual_memory().percent,
#         qsize=qsize,
#         qusage=usage / 100,
#         workers=len(self.workers),
#     )
#
#-----------------------------------------------------------



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
        min_workers: int = 5,  # Increased from 3 to 5 for better baseline
        max_workers: int = 20,  # Decreased from 30 to 15 for cost efficiency
        ewma_alpha: float = 0.2,
        queue_check_interval: float = 2.0,
        queue_size: int = 2000,
        shutdown_wait_seconds: float = 10.0,
        metrics_cb=None
    ):
        self.min_workers = max(1, min_workers)
        self.max_workers = max(self.min_workers, max_workers)
        self.ewma_alpha = max(0.0, min(1.0, ewma_alpha))
        self.queue_check_interval = max(0.1, queue_check_interval)
        self.shutdown_wait_seconds = shutdown_wait_seconds
        self.metrics_cb = metrics_cb

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

        # Advanced scaling metrics for optimization
        self._messages_processed = 0
        self._last_messages_processed = 0
        self._processing_rate = 0.0  # Messages per second
        self._steady_state_queue = 0.0  # Moving average for steady-state detection
        self._steady_alpha = 0.1  # Smoothing factor for steady-state
        self._last_scale_up_time = 0.0  # Timestamp of last scale-up
        self._scale_up_cooldown = 8.0  # Cooldown period after scale-up (seconds)
        self._consecutive_idle_intervals = 0  # Track consecutive idle periods

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

                        # Track successful message processing
                        self._messages_processed += 1
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

            # Metrics callback
            if self.metrics_cb:
                self.metrics_cb(
                    cpu=self._cpu_ewma,
                    mem=psutil.virtual_memory().percent,
                    qsize=qsize,
                    qusage=usage / 100,
                    workers=len(self.workers),
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
            # Calculate processing rate and steady-state metrics
            # -------------------------
            import time
            current_time = time.time()

            # Calculate processing rate (msgs/sec)
            msgs_processed_delta = self._messages_processed - self._last_messages_processed
            self._processing_rate = msgs_processed_delta / self.queue_check_interval
            self._last_messages_processed = self._messages_processed

            # Update steady-state queue baseline (moving average)
            self._steady_state_queue = (
                self._steady_alpha * qsize +
                (1.0 - self._steady_alpha) * self._steady_state_queue
            )

            # Calculate worker efficiency (msgs per worker per second)
            worker_count = len(self.workers)
            worker_efficiency = self._processing_rate / worker_count if worker_count > 0 else 0.0

            # Estimate queue drain time with current workers
            estimated_drain_time = qsize / self._processing_rate if self._processing_rate > 10 else 999

            # Calculate worker idle percentage
            # If processing rate < incoming rate estimate, workers may be busy
            # Rough estimate: if queue growing, workers are busy
            worker_idle_pct = max(0, 100 * (1 - abs(delta_q) / (qsize + 1)))

            # Debug log with new metrics
            logger.debug(
                f"[Scaler] q={qsize}, Δq={delta_q}, usage={usage:.1f}%, "
                f"proc_rate={self._processing_rate:.1f}msg/s, "
                f"worker_eff={worker_efficiency:.1f}msg/w/s, "
                f"steady={self._steady_state_queue:.0f}, "
                f"workers={worker_count}, CPU={self._cpu_ewma:.1f}%"
            )

            # -------------------------
            # Scale-up triggers with OPTIMIZED logic:
            #  1. Check cooldown period (don't scale too frequently)
            #  2. Detect if queue spike is above steady-state baseline
            #  3. Check if workers can handle load (processing rate vs queue)
            #  4. Verify workers are actually busy (not idle)
            # -------------------------

            # Check cooldown - don't scale up if recently scaled
            time_since_last_scale = current_time - self._last_scale_up_time
            in_cooldown = time_since_last_scale < self._scale_up_cooldown

            # Detect significant spike ABOVE steady-state
            steady_threshold = max(self._steady_state_queue * 2, 200)  # At least 2x steady or 200
            spike_detected = (qsize > steady_threshold and delta_q > 100)

            # Persistent high queue (above 40% capacity for extended period)
            persistent_high = (usage >= 40.0 and qsize > 300)

            # Check if workers are keeping up
            # If drain time > 5 seconds, we need more workers
            workers_not_keeping_up = (estimated_drain_time > 5.0 and qsize > 200)

            # Check worker efficiency - if too low, might need more workers
            low_efficiency = (worker_efficiency < 8.0 and qsize > 200)  # <8 msgs/worker/sec

            # Combine scale-up triggers
            scale_up_trigger = (
                (spike_detected or persistent_high or workers_not_keeping_up or low_efficiency)
                and not in_cooldown
            )

            # Only scale up if:
            # - Trigger conditions met
            # - CPU not overloaded (< 80%)
            # - Load average reasonable (< 2.5)
            # - Haven't reached max workers
            # - Queue is actually accumulating (not being drained)
            if (
                scale_up_trigger
                and self._cpu_ewma < 80.0
                and load_avg < 2.5
                and worker_count < self.max_workers
                and qsize > 100  # Meaningful queue size
            ):
                self._start_worker()
                self._last_scale_up_time = current_time
                logger.info(
                    f"SCALE UP: +1 worker. Queue={qsize}, ΔQ={delta_q}, "
                    f"ProcRate={self._processing_rate:.1f}msg/s, "
                    f"Workers={worker_count+1}, Reason={'spike' if spike_detected else 'persistent' if persistent_high else 'not_keeping_up'}"
                )
                # Reset low queue counter since we just scaled up
                self._low_queue_intervals = 0
                self._consecutive_idle_intervals = 0
                # continue to next iteration to avoid immediate scale-down logic
                continue

            # -------------------------
            # Scale-down logic: MORE AGGRESSIVE for cost optimization
            # Remove workers when clearly over-provisioned
            # -------------------------
            
            # Track consecutive low/idle periods
            low_queue_threshold = 50  # Increased threshold
            if qsize < low_queue_threshold:
                self._low_queue_intervals += 1
            else:
                self._low_queue_intervals = 0
                
            # Track completely idle periods
            if qsize == 0:
                self._consecutive_idle_intervals += 1
            else:
                self._consecutive_idle_intervals = 0

            # Scale down conditions (multiple triggers for efficiency):
            if worker_count > self.min_workers:
                # Condition 1: Queue consistently low + low CPU
                low_queue_scale_down = (
                    self._low_queue_intervals >= 3 and  # 6 seconds
                    self._cpu_ewma < 25.0 and
                    qsize < 20
                )
                
                # Condition 2: Queue completely empty for extended period
                idle_scale_down = (
                    self._consecutive_idle_intervals >= 4 and  # 8 seconds
                    qsize == 0
                )
                
                # Condition 3: High worker efficiency (workers are idle)
                # If we're processing very fast with high worker count, scale down
                over_provisioned = (
                    worker_efficiency < 5.0 and  # Very low work per worker
                    worker_count > self.min_workers + 2 and
                    qsize < self._steady_state_queue * 1.5  # Queue near steady-state
                )
                
                # Condition 4: Processing rate significantly exceeds apparent incoming rate
                # If queue is stable or decreasing, and we have many workers, scale down
                processing_exceeds_incoming = (
                    delta_q <= 0 and  # Queue not growing
                    worker_count > self.min_workers + 3 and
                    qsize < 100 and
                    self._processing_rate > 50  # Decent processing happening
                )
                
                # Trigger scale-down if any condition met
                scale_down_trigger = (
                    low_queue_scale_down or 
                    idle_scale_down or 
                    over_provisioned or
                    processing_exceeds_incoming
                )

                if scale_down_trigger:
                    # Remove the last worker in the list (LIFO)
                    worker_id, task = self.workers.pop()
                    if not task.done():
                        task.cancel()
                    logger.info(
                        f"SCALE DOWN: -1 worker. Queue={qsize}, "
                        f"ProcRate={self._processing_rate:.1f}msg/s, "
                        f"WorkerEff={worker_efficiency:.1f}msg/w/s, "
                        f"Workers={worker_count-1}, "
                        f"Reason={'low_queue' if low_queue_scale_down else 'idle' if idle_scale_down else 'over_provisioned' if over_provisioned else 'exceeds_incoming'}"
                    )
                    # reset counters to avoid removing too many at once
                    self._low_queue_intervals = 0
                    self._consecutive_idle_intervals = 0

        logger.debug("Autoscaler exiting.")
