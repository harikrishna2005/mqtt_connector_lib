# tests/test_mqtt_connector_lib/test_smart_scaling_executor_consumer.py
import pytest
import asyncio
import time
import pytest_asyncio
from mqtt_connector_lib.smart_scaling_executor import SmartScalingExecutor


class TestSmartScalingExecutorConsumerPerspective:
    """Test SmartScalingExecutor from consumer/developer usage perspective"""

    @pytest_asyncio.fixture
    async def executor(self):
        """Setup executor for consumer testing"""
        executor = SmartScalingExecutor(
            min_workers=3,
            max_workers=10,
            queue_size=100,
            queue_check_interval=0.5,
            shutdown_wait_seconds=5.0
        )
        await executor.start()
        yield executor
        await executor.stop()

    @pytest.mark.asyncio
    async def test_consumer_submits_tasks_and_processes_with_min_workers(self, executor):
        """Test: Consumer submits tasks and they are processed with minimum workers"""
        results = []

        async def consumer_task_handler(topic: str, payload: str, handler):
            """Consumer's task handler"""
            await asyncio.sleep(0.1)
            results.append({"topic": topic, "payload": payload})

        # Consumer submits tasks
        for i in range(5):
            executor.submit(f"topic_{i}", f"payload_{i}", consumer_task_handler)

        await asyncio.sleep(1.0)

        # Verify all tasks processed
        assert len(results) == 5
        assert executor.get_queue_size() == 0

    @pytest.mark.asyncio
    async def test_consumer_experiences_scale_up_during_burst(self):
        """Test: Consumer sees worker scale-up during message burst"""
        executor = SmartScalingExecutor(
            min_workers=2,
            max_workers=8,
            queue_size=200,
            queue_check_interval=0.3
        )
        await executor.start()

        processed = []
        initial_workers = len(executor.workers)

        async def burst_handler(topic: str, payload: str, handler):
            await asyncio.sleep(0.2)
            processed.append(payload)

        # Simulate burst: submit 50 messages rapidly
        for i in range(50):
            executor.submit(f"burst/topic", f"msg_{i}", burst_handler)

        # Allow autoscaler to detect spike and scale up
        await asyncio.sleep(2.0)

        peak_workers = len(executor.workers)

        # Wait for processing to complete
        await asyncio.sleep(5.0)

        await executor.stop()

        # Verify scale-up occurred
        assert peak_workers > initial_workers, f"Expected scale-up from {initial_workers}, got {peak_workers}"
        assert len(processed) == 50

    @pytest.mark.asyncio
    async def test_consumer_experiences_scale_down_after_idle_period(self):
        """Test: Consumer sees worker scale-down after idle period"""
        executor = SmartScalingExecutor(
            min_workers=2,
            max_workers=8,
            queue_size=100,
            queue_check_interval=0.5
        )
        await executor.start()

        processed = []

        async def task_handler(topic: str, payload: str, handler):
            await asyncio.sleep(0.1)
            processed.append(payload)

        # Create initial load to trigger scale-up
        for i in range(30):
            executor.submit("load/topic", f"msg_{i}", task_handler)

        await asyncio.sleep(1.5)
        workers_after_load = len(executor.workers)

        # Wait for queue to drain and scale-down to occur
        await asyncio.sleep(4.0)

        final_workers = len(executor.workers)

        await executor.stop()

        # Verify scale-down occurred
        assert final_workers < workers_after_load, f"Expected scale-down from {workers_after_load}, got {final_workers}"
        assert final_workers >= executor.min_workers

    @pytest.mark.asyncio
    async def test_consumer_handles_queue_overflow_with_drop_policy(self, executor):
        """Test: Consumer handles queue overflow with message dropping"""
        processed = []
        dropped_count = 0

        async def slow_handler(topic: str, payload: str, handler):
            await asyncio.sleep(0.5)
            processed.append(payload)

        # Fill queue beyond capacity
        for i in range(120):  # Queue size is 100
            success = executor.submit("overflow/topic", f"msg_{i}", slow_handler)
            if not success:
                dropped_count += 1

        await asyncio.sleep(10.0)

        # Verify some messages were dropped
        assert dropped_count > 0, "Expected some messages to be dropped due to queue overflow"
        assert len(processed) < 120

    @pytest.mark.asyncio
    async def test_consumer_monitors_queue_usage_warnings(self):
        """Test: Consumer receives queue usage warnings at thresholds"""
        executor = SmartScalingExecutor(
            min_workers=1,
            max_workers=3,
            queue_size=50,
            queue_check_interval=0.3
        )
        await executor.start()

        async def very_slow_handler(topic: str, payload: str, handler):
            await asyncio.sleep(1.0)

        # Fill queue to trigger warnings (70%, 85%, 95%)
        for i in range(48):  # 96% of queue
            executor.submit("warning/topic", f"msg_{i}", very_slow_handler)

        # Allow autoscaler to log warnings
        await asyncio.sleep(2.0)

        queue_usage = (executor.get_queue_size() / 50) * 100

        await executor.stop()

        # Verify queue reached high usage
        assert queue_usage >= 70.0, f"Expected queue usage >= 70%, got {queue_usage:.1f}%"

    @pytest.mark.asyncio
    async def test_consumer_uses_sync_handler_functions(self, executor):
        """Test: Consumer can use synchronous handler functions"""
        results = []

        def sync_handler(topic: str, payload: str):
            """Synchronous consumer handler"""
            results.append({"topic": topic, "payload": payload, "sync": True})

        # Submit tasks with sync handler
        for i in range(5):
            executor.submit(f"sync/topic_{i}", f"sync_payload_{i}", sync_handler)

        await asyncio.sleep(1.0)

        # Verify sync handlers executed
        assert len(results) == 5
        assert all(r["sync"] for r in results)

    @pytest.mark.asyncio
    async def test_consumer_handles_handler_exceptions_gracefully(self, executor):
        """Test: Consumer's handlers throw exceptions without crashing workers"""
        successful = []
        error_count = 0

        async def faulty_handler(topic: str, payload: str, handler):
            nonlocal error_count
            if "error" in payload:
                error_count += 1
                raise ValueError(f"Processing failed: {payload}")
            successful.append(payload)

        # Mix of good and bad messages
        executor.submit("test/topic", "good_msg_1", faulty_handler)
        executor.submit("test/topic", "error_msg", faulty_handler)
        executor.submit("test/topic", "good_msg_2", faulty_handler)

        await asyncio.sleep(1.0)

        # Verify good messages processed, bad ones logged
        assert len(successful) == 2
        assert error_count == 1

    @pytest.mark.asyncio
    async def test_consumer_graceful_shutdown_completes_inflight_tasks(self):
        """Test: Consumer's in-flight tasks complete during graceful shutdown"""
        executor = SmartScalingExecutor(
            min_workers=3,
            max_workers=5,
            queue_size=50,
            shutdown_wait_seconds=10.0
        )
        await executor.start()

        started = []
        completed = []

        async def long_task_handler(topic: str, payload: str, handler):
            started.append(payload)
            await asyncio.sleep(0.5)
            completed.append(payload)

        # Submit tasks
        for i in range(10):
            executor.submit("shutdown/topic", f"task_{i}", long_task_handler)

        # Allow tasks to start
        await asyncio.sleep(0.3)

        # Initiate shutdown
        await executor.stop()

        # Verify tasks completed
        assert len(started) >= 3  # At least min_workers started
        assert len(completed) >= 3  # Tasks completed during graceful shutdown

    @pytest.mark.asyncio
    async def test_consumer_high_throughput_concurrent_processing(self):
        """Test: Consumer handles high-throughput with concurrent processing"""
        executor = SmartScalingExecutor(
            min_workers=5,
            max_workers=15,
            queue_size=500,
            queue_check_interval=0.5
        )
        await executor.start()

        processed = []
        processing_times = []

        async def iot_handler(topic: str, payload: str, handler):
            start = time.time()
            await asyncio.sleep(0.05)
            processing_times.append(time.time() - start)
            processed.append(payload)

        # Submit 100 messages
        for i in range(100):
            executor.submit("iot/devices", f"device_{i:03d}", iot_handler)

        # Wait for processing
        await asyncio.sleep(5.0)

        await executor.stop()

        # Verify concurrent processing
        assert len(processed) == 100
        total_time = sum(processing_times)
        # With concurrency, should be much faster than sequential (5 seconds)
        assert total_time < 2.0

    @pytest.mark.asyncio
    async def test_consumer_monitors_queue_size_metrics(self, executor):
        """Test: Consumer monitors queue size for observability"""
        queue_sizes = []

        async def monitored_handler(topic: str, payload: str, handler):
            await asyncio.sleep(0.1)
            queue_sizes.append(executor.get_queue_size())

        # Submit tasks gradually
        for i in range(10):
            executor.submit("metrics/topic", f"msg_{i}", monitored_handler)
            await asyncio.sleep(0.05)

        await asyncio.sleep(2.0)

        # Verify queue size tracking
        assert len(queue_sizes) > 0
        assert max(queue_sizes) <= 10

    @pytest.mark.asyncio
    async def test_consumer_worker_ids_monotonically_increase(self):
        """Test: Consumer observes monotonically increasing worker IDs (no reuse)"""
        executor = SmartScalingExecutor(
            min_workers=2,
            max_workers=10,
            queue_size=200,
            queue_check_interval=0.3
        )
        await executor.start()

        worker_ids_seen = set()

        async def tracking_handler(topic: str, payload: str, handler):
            await asyncio.sleep(0.2)

        # Trigger scale-up
        for i in range(50):
            executor.submit("scale/topic", f"msg_{i}", tracking_handler)

        await asyncio.sleep(2.0)

        # Collect worker IDs during peak
        for wid, _ in executor.workers:
            worker_ids_seen.add(wid)

        initial_ids = worker_ids_seen.copy()

        # Wait for scale-down
        await asyncio.sleep(4.0)

        # Trigger another scale-up
        for i in range(50):
            executor.submit("scale/topic", f"msg_{i}_2", tracking_handler)

        await asyncio.sleep(2.0)

        # Collect worker IDs again
        for wid, _ in executor.workers:
            worker_ids_seen.add(wid)

        await executor.stop()

        # Verify worker IDs never reused
        assert len(worker_ids_seen) > len(initial_ids), "Expected new worker IDs after scale-down/up cycle"

    @pytest.mark.asyncio
    async def test_consumer_handles_max_workers_limit(self):
        """Test: Consumer respects max_workers limit during extreme load"""
        executor = SmartScalingExecutor(
            min_workers=2,
            max_workers=5,
            queue_size=200,
            queue_check_interval=0.2
        )
        await executor.start()

        async def handler(topic: str, payload: str, handler_func):
            await asyncio.sleep(0.3)

        # Submit massive burst
        for i in range(100):
            executor.submit("max/topic", f"msg_{i}", handler)

        # Allow autoscaler to react
        await asyncio.sleep(2.0)

        max_workers_reached = len(executor.workers)

        await executor.stop()

        # Verify max_workers not exceeded
        assert max_workers_reached <= 5, f"Expected workers <= 5, got {max_workers_reached}"
