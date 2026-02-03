import asyncio
import random
import time


class LoadGenerator:
    """
    Generates artificial load and submits tasks to SmartScalingExecutor.
    """

    def __init__(self, executor, rate=200, burst_sizes=None, burst_interval=5, burst_interval_range=None):
        """
        rate = messages per second (for continuous load)
        burst_sizes = list of burst sizes to randomly choose from (default: [200, 500])
        burst_interval = seconds between random bursts (if burst_interval_range not set)
        burst_interval_range = tuple of (min, max) seconds for random intervals (e.g., (3, 5))
        """
        self.executor = executor
        self.rate = rate
        self.burst_sizes = burst_sizes if burst_sizes else [200, 500]
        self.burst_interval = burst_interval
        self.burst_interval_range = burst_interval_range  # e.g., (3, 5) for random 3-5 seconds
        self._stop = False
        self._burst_count = 0

    async def start(self):
        interval_desc = f"{self.burst_interval_range[0]}-{self.burst_interval_range[1]}s" if self.burst_interval_range else f"{self.burst_interval}s"
        print(f"[LOAD] Load generator started - will randomly submit bursts of {self.burst_sizes} messages every {interval_desc}")

        # Submit initial burst
        await self._submit_random_burst()

        # Schedule periodic random bursts in background
        asyncio.create_task(self._periodic_burst_generator())

        # Continue with steady load generation
        print(f"[LOAD] Now generating steady load at {self.rate} msgs/sec...")
        while not self._stop:
            start = time.time()

            for _ in range(self.rate):
                self.executor.submit(
                    "demo/topic",
                    payload={"value": random.randint(1, 99999)},
                    handler=self._demo_handler
                )

            # Keep exact rate
            elapsed = time.time() - start
            delay = max(0, 1.0 - elapsed)
            await asyncio.sleep(delay)

    async def _periodic_burst_generator(self):
        """Periodically generate random burst loads with variable intervals"""
        while not self._stop:
            # Use random interval if range specified, otherwise fixed interval
            if self.burst_interval_range:
                interval = random.uniform(self.burst_interval_range[0], self.burst_interval_range[1])
            else:
                interval = self.burst_interval

            await asyncio.sleep(interval)
            if not self._stop:
                await self._submit_random_burst()

    async def _submit_random_burst(self):
        """Submit a random burst of messages"""
        burst_size = random.choice(self.burst_sizes)
        self._burst_count += 1

        print(f"\n[LOAD] 🚀 BURST #{self._burst_count}: Submitting {burst_size} messages...")
        burst_start = time.time()
        submitted = 0
        dropped = 0

        for i in range(burst_size):
            success = self.executor.submit(
                "demo/topic",
                payload={"value": random.randint(1, 99999), "burst": self._burst_count, "msg_id": i},
                handler=self._demo_handler
            )
            if success:
                submitted += 1
            else:
                dropped += 1

        burst_elapsed = time.time() - burst_start
        queue_size = self.executor.get_queue_size()
        print(f"[LOAD] ✅ Burst #{self._burst_count} complete in {burst_elapsed:.3f}s: "
              f"{submitted} submitted, {dropped} dropped | Queue size: {queue_size}")

        # Small delay to allow async processing
        await asyncio.sleep(0.01)

    async def stop(self):
        self._stop = True
        print("[LOAD] Load generator stopped")

    async def _demo_handler(self, topic, payload):
        """
        A lightweight artificial processing task.
        """
        await asyncio.sleep(random.uniform(0.001, 0.010))
