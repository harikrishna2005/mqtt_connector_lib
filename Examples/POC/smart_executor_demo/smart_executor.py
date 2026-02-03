import asyncio
import time
import psutil
import os
import inspect


class SmartScalingExecutor:
    """
    A smart auto-scaling executor that:
    - Manages worker tasks
    - Scales based on queue delta, CPU EWMA, memory %, queue %
    - Tracks EWMA CPU load
    - Provides queue usage monitoring
    """

    def __init__(
        self,
        min_workers=3,
        max_workers=20,
        queue_size=1000,
        scaler_interval=2.0,
        ewma_alpha=0.3,
        warn_70=0.7,
        warn_85=0.85,
        warn_95=0.95,
        metrics_cb=None,
    ):
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.scaler_interval = scaler_interval
        self.queue = asyncio.Queue(maxsize=queue_size)
        self.queue_size_limit = queue_size

        self.ewma_alpha = ewma_alpha
        self._cpu_ewma = 0.0

        self.warn_70 = warn_70
        self.warn_85 = warn_85
        self.warn_95 = warn_95

        self.metrics_cb = metrics_cb

        self.workers = []
        self._next_worker_id = 0
        self._stop = False

        self._last_q_size = 0
        self._low_q_intervals = 0

    # -------------------------------------------------------
    # Public API
    # -------------------------------------------------------

    async def start(self):
        for _ in range(self.min_workers):
            self._start_worker()

        asyncio.create_task(self._auto_scaler())

    def _start_worker(self):
        wid = self._next_worker_id
        self._next_worker_id += 1

        worker = asyncio.create_task(self._worker_loop(wid))
        self.workers.append(worker)
        print(f"[EXEC] Worker-{wid} started. Total={len(self.workers)}")

    async def stop(self):
        self._stop = True

        for w in self.workers:
            w.cancel()

        try:
            await asyncio.gather(*self.workers, return_exceptions=True)
        except Exception:
            pass

        print("[EXEC] All workers stopped")

    async def submit(self, item):
        try:
            self.queue.put_nowait(item)
        except asyncio.QueueFull:
            raise RuntimeError("Queue is full. Consider increasing queue size or scaling workers.")

    # -------------------------------------------------------
    # Worker Logic
    # -------------------------------------------------------

    async def _worker_loop(self, wid):
        name = f"worker-{wid}"
        try:
            while not self._stop:
                task = await self.queue.get()
                handler, payload = task

                try:
                    res = handler(payload)
                    if inspect.isawaitable(res):
                        await res
                except Exception as e:
                    print(f"[ERR] {name} handler error:", e)

                self.queue.task_done()

        except asyncio.CancelledError:
            print(f"[EXEC] {name} cancelled")
            return

    # -------------------------------------------------------
    # Auto-scaling Logic
    # -------------------------------------------------------

    async def _auto_scaler(self):
        while not self._stop:
            q_size = self.queue.qsize()

            # QUEUE DELTA
            delta_q = q_size - self._last_q_size
            self._last_q_size = q_size

            # CPU EWMA
            cpu_now = psutil.cpu_percent(interval=None)
            self._cpu_ewma = (self.ewma_alpha * cpu_now) + (
                (1 - self.ewma_alpha) * self._cpu_ewma
            )

            # MEMORY %
            mem_now = psutil.virtual_memory().percent

            # QUEUE USAGE %
            usage = q_size / self.queue_size_limit

            # metrics callback
            if self.metrics_cb:
                self.metrics_cb(
                    cpu=self._cpu_ewma,
                    mem=mem_now,
                    qsize=q_size,
                    qusage=usage,
                    workers=len(self.workers),
                )

            # queue low periods
            if q_size < 5:
                self._low_q_intervals += 1
            else:
                self._low_q_intervals = 0

            # -------------------------------------------------------
            # SCALE UP
            # -------------------------------------------------------

            scale_up_trigger = (
                delta_q > 20
                or (self._cpu_ewma >= 60 and usage >= self.warn_70)
            )

            load_avg = 0.0
            try:
                load_avg = os.getloadavg()[0]
            except Exception:
                pass

            if (
                scale_up_trigger
                and self._cpu_ewma < 70
                and load_avg < 1.2
                and len(self.workers) < self.max_workers
            ):
                self._start_worker()

            # -------------------------------------------------------
            # SCALE DOWN
            # -------------------------------------------------------

            if (
                q_size < 5
                and self._low_q_intervals >= 5
                and len(self.workers) > self.min_workers
            ):
                worker = self.workers.pop()
                worker.cancel()
                print(f"[EXEC] Worker removed. Total={len(self.workers)}")
                self._low_q_intervals = 0

            await asyncio.sleep(self.scaler_interval)
