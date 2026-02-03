import asyncio
import random
import time


class LoadGenerator:
    """
    Generates artificial load and submits tasks to SmartScalingExecutor.
    """

    def __init__(self, executor, rate=200):
        """
        rate = messages per second
        """
        self.executor = executor
        self.rate = rate
        self._stop = False

    async def start(self):
        print("[LOAD] Load generator started")
        while not self._stop:
            start = time.time()

            for _ in range(self.rate):
                # Dummy handler that sleeps randomly
                await self.executor.submit(
                    (self._demo_handler, {"value": random.randint(1, 99999)})
                )

            # Keep exact rate
            elapsed = time.time() - start
            delay = max(0, 1.0 - elapsed)
            await asyncio.sleep(delay)

    async def stop(self):
        self._stop = True
        print("[LOAD] Load generator stopped")

    async def _demo_handler(self, payload):
        """
        A lightweight artificial processing task.
        """
        await asyncio.sleep(random.uniform(0.001, 0.010))
