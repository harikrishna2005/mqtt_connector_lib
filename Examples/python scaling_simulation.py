import asyncio
import random
import time
from mqtt_connector_lib.smart_scaling_executor import SmartScalingExecutor

# ============================================================================
# Scaling Simulation Script
# ============================================================================
# This script demonstrates SmartScalingExecutor's auto-scaling behavior
# under varying load conditions.
#
# Key Update: Handler retrieval moved to worker loop for better throughput
# - Handlers are registered once in topic_handlers dict
# - submit() only accepts topic and payload (no handler parameter)
# - Workers retrieve handlers from dict inside their loop
# - Result: Non-blocking message submission, better concurrency
# ============================================================================

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

        # Submit only topic and payload (handler retrieved in worker loop)
        submitted = executor.submit(topic, f"msg-{count}")
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
    # Create topic handlers dictionary
    topic_handlers = {
        "test/topic": fake_handler
    }

    # Pass topic_handlers to executor during initialization
    executor = SmartScalingExecutor(
        topic_handlers=topic_handlers,  # NEW: Pass handlers dict
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
