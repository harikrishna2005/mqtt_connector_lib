import asyncio
import random
import time
from mqtt_connector_lib.smart_scaling_executor import SmartScalingExecutor

# A fake message handler that simulates variable work
async def fake_handler(topic, payload):
    # Simulate handler workload: sometimes fast, sometimes slow
    work_time = random.uniform(0.01, 0.15)
    await asyncio.sleep(work_time)

async def producer(executor: SmartScalingExecutor):
    """
    Pushes tasks into the executor queue at varying speeds
    to simulate real-world load surges.
    """
    topic = "test/topic"
    count = 0

    while True:
        count += 1

        # Simulate LOW load
        if count < 200:
            await asyncio.sleep(0.02)  # ~50 messages/sec

        # Simulate SPIKE load
        elif 200 <= count < 600:
            await asyncio.sleep(0.002)  # ~500 messages/sec

        # Simulate moderate load
        elif 600 <= count < 900:
            await asyncio.sleep(0.01)

        # Simulate drop to zero
        else:
            await asyncio.sleep(0.2)

        submitted = executor.submit(topic, f"msg-{count}", fake_handler)
        if not submitted:
            print(f"[DROP] Queue full at message {count}")

async def monitor(executor: SmartScalingExecutor):
    """
    Periodically prints queue size and worker count.
    Useful for watching scaling behavior.
    """
    while True:
        qsize = executor.get_queue_size()
        wcount = len(executor.workers)
        cpu = executor._cpu_ewma

        print(f"[MONITOR] queue={qsize} workers={wcount} cpuEWMA={cpu:.1f}%")
        await asyncio.sleep(1)

async def main():
    executor = SmartScalingExecutor(
        min_workers=3,
        max_workers=12,
        queue_size=500,
        queue_check_interval=2.0,
    )

    await executor.start()

    await asyncio.gather(
        producer(executor),
        monitor(executor),
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopping...")
