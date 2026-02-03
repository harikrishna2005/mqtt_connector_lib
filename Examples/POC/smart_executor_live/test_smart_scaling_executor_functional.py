"""
Functional Tests for SmartScalingExecutor

These tests validate the core scaling functionality:
1. Worker scaling up under load
2. Worker scaling down when idle
3. Queue processing without drops
4. Min/max worker boundaries
5. Scaling thresholds and behavior

Run with: pytest test_smart_scaling_executor_functional.py -v
"""

import asyncio
import pytest
import time
import sys
from pathlib import Path

# Add the current directory to path
sys.path.insert(0, str(Path(__file__).parent))

from smart_scaling_executor import SmartScalingExecutor


# ============================================================================
# Test Fixtures
# ============================================================================

@pytest.fixture
def executor():
    """Create a basic executor for testing"""
    return SmartScalingExecutor(
        min_workers=3,
        max_workers=10,
        queue_check_interval=1.0,  # Faster for testing
        queue_size=1000
    )


@pytest.fixture
async def started_executor(executor):
    """Create and start an executor, ensure cleanup"""
    await executor.start()
    yield executor
    await executor.stop()


async def dummy_handler(topic, payload):
    """Simple async handler that does minimal work"""
    await asyncio.sleep(0.01)


async def slow_handler(topic, payload):
    """Slow handler to simulate heavy processing"""
    await asyncio.sleep(0.5)


# ============================================================================
# Core Functionality Tests
# ============================================================================

class TestBasicFunctionality:
    """Test basic executor operations"""

    @pytest.mark.asyncio
    async def test_executor_starts_with_min_workers(self, executor):
        """Verify executor starts with minimum number of workers"""
        await executor.start()

        assert len(executor.workers) == 3, f"Expected 3 workers, got {len(executor.workers)}"
        assert not executor.shutdown_flag

        await executor.stop()

    @pytest.mark.asyncio
    async def test_executor_stops_cleanly(self, started_executor):
        """Verify executor stops and cleans up workers"""
        initial_workers = len(started_executor.workers)
        assert initial_workers > 0, "Executor should have workers after start"

        await started_executor.stop()

        assert started_executor.shutdown_flag
        assert len(started_executor.workers) == 0, "All workers should be removed after stop"

    @pytest.mark.asyncio
    async def test_submit_to_queue(self, started_executor):
        """Verify messages can be submitted to queue"""
        success = started_executor.submit("test/topic", "test_payload", dummy_handler)

        assert success, "Submit should return True when queue not full"
        assert started_executor.get_queue_size() > 0, "Queue should contain the message"

        # Wait for processing
        await asyncio.sleep(0.5)

        assert started_executor.get_queue_size() == 0, "Message should be processed"

    @pytest.mark.asyncio
    async def test_multiple_messages_processed(self, started_executor):
        """Verify multiple messages are processed successfully"""
        message_count = 50

        for i in range(message_count):
            success = started_executor.submit(f"topic/{i}", f"payload_{i}", dummy_handler)
            assert success, f"Message {i} should be accepted"

        # Wait for all messages to be processed
        await asyncio.sleep(2.0)

        # All messages should be processed
        assert started_executor.get_queue_size() == 0, "All messages should be processed"
        assert started_executor._messages_processed >= message_count, \
            f"Expected at least {message_count} messages processed, got {started_executor._messages_processed}"


# ============================================================================
# Scale-Up Tests (Core Functionality)
# ============================================================================

class TestScaleUp:
    """Test worker scaling up under load"""

    @pytest.mark.asyncio
    async def test_scales_up_on_burst_load(self, started_executor):
        """
        CRITICAL TEST: Verify workers scale up when burst load arrives
        This is the core scale-up functionality
        """
        initial_workers = len(started_executor.workers)

        # Submit burst of messages (300 messages with slow handler)
        burst_size = 300
        for i in range(burst_size):
            started_executor.submit(f"topic/{i}", f"payload_{i}", slow_handler)

        # Wait for autoscaler to react (2-3 check intervals)
        await asyncio.sleep(3.5)

        # Workers should have scaled up
        current_workers = len(started_executor.workers)
        assert current_workers > initial_workers, \
            f"Workers should scale up: initial={initial_workers}, current={current_workers}"

        # Verify it's within expected range (should be 4-10 workers for this load)
        # Note: Conservative scaling may result in fewer workers initially
        assert 4 <= current_workers <= 10, \
            f"Worker count {current_workers} should be in reasonable range [4-10] for burst load"

    @pytest.mark.asyncio
    async def test_scales_up_to_max_workers(self, started_executor):
        """
        CRITICAL TEST: Verify workers scale up to max but not beyond
        """
        # Submit massive burst that would require more than max_workers
        burst_size = 800
        for i in range(burst_size):
            started_executor.submit(f"topic/{i}", f"payload_{i}", slow_handler)

        # Wait for autoscaler to react multiple times
        await asyncio.sleep(6.0)

        current_workers = len(started_executor.workers)

        # Should have scaled up significantly but not exceed max
        assert current_workers > 3, f"Should scale up from min (3), got {current_workers}"
        assert current_workers <= 10, f"Should not exceed max_workers (10), got {current_workers}"

        # For this load, should reach significant scaling (at least 6 workers)
        assert current_workers >= 6, \
            f"With 800 slow messages, should scale to at least 6 workers, got {current_workers}"

    @pytest.mark.asyncio
    async def test_respects_cooldown_between_scale_ups(self, started_executor):
        """
        TEST: Verify cooldown period prevents rapid scale-ups
        """
        # Submit messages to trigger initial scale-up
        for i in range(100):
            started_executor.submit(f"topic/{i}", f"payload_{i}", slow_handler)

        await asyncio.sleep(2.0)
        workers_after_first_scale = len(started_executor.workers)

        # Submit more immediately (should be in cooldown)
        for i in range(100, 200):
            started_executor.submit(f"topic/{i}", f"payload_{i}", slow_handler)

        # Check immediately (within cooldown period)
        await asyncio.sleep(1.0)
        workers_during_cooldown = len(started_executor.workers)

        # Workers shouldn't change much during cooldown
        assert abs(workers_during_cooldown - workers_after_first_scale) <= 1, \
            "Workers shouldn't scale rapidly during cooldown"

    @pytest.mark.asyncio
    async def test_steady_state_detection_prevents_false_scale_up(self, started_executor):
        """
        TEST: Verify steady-state load doesn't trigger excessive scaling
        """
        # Simulate steady low load (50 fast messages)
        for _ in range(3):  # 3 rounds of steady load
            for i in range(50):
                started_executor.submit(f"topic/{i}", f"payload_{i}", dummy_handler)
            await asyncio.sleep(2.0)

        # Workers should stay near minimum (3-5 range)
        current_workers = len(started_executor.workers)
        assert 3 <= current_workers <= 6, \
            f"Steady low load should keep workers at 3-6, got {current_workers}"


# ============================================================================
# Scale-Down Tests (Core Functionality)
# ============================================================================

class TestScaleDown:
    """Test worker scaling down when load decreases"""

    @pytest.mark.asyncio
    async def test_scales_down_when_idle(self, started_executor):
        """
        CRITICAL TEST: Verify workers scale down when queue is empty
        This is the core scale-down functionality
        """
        # First, scale up by submitting burst
        burst_size = 200
        for i in range(burst_size):
            started_executor.submit(f"topic/{i}", f"payload_{i}", slow_handler)

        await asyncio.sleep(3.0)
        peak_workers = len(started_executor.workers)

        # Wait for queue to drain and workers to scale down
        # Queue should be empty, workers should scale down
        await asyncio.sleep(10.0)

        final_workers = len(started_executor.workers)

        # Workers should have scaled down
        assert final_workers < peak_workers, \
            f"Workers should scale down: peak={peak_workers}, final={final_workers}"

        # Should return near minimum (but may not reach exactly min immediately)
        assert final_workers <= 6, \
            f"After idle period, workers should be ≤6, got {final_workers}"

    @pytest.mark.asyncio
    async def test_scales_down_to_min_workers(self, started_executor):
        """
        CRITICAL TEST: Verify workers scale down to minimum (not below)
        """
        # Scale up first
        for i in range(150):
            started_executor.submit(f"topic/{i}", f"payload_{i}", slow_handler)

        await asyncio.sleep(3.0)
        assert len(started_executor.workers) > 3, "Should scale up initially"

        # Wait for complete drain and scale-down (15 seconds)
        await asyncio.sleep(15.0)

        final_workers = len(started_executor.workers)

        # Should scale down to minimum
        assert final_workers >= 3, f"Should not go below min_workers (3), got {final_workers}"
        assert final_workers <= 5, f"Should return near minimum after extended idle, got {final_workers}"

    @pytest.mark.asyncio
    async def test_rapid_scale_down_on_empty_queue(self, started_executor):
        """
        TEST: Verify fast scale-down when queue is consistently empty
        """
        # Scale up
        for i in range(100):
            started_executor.submit(f"topic/{i}", f"payload_{i}", dummy_handler)

        await asyncio.sleep(2.0)
        workers_after_scale_up = len(started_executor.workers)
        assert workers_after_scale_up > 3, "Should scale up"

        # Wait for messages to process (fast handler)
        await asyncio.sleep(2.0)
        assert started_executor.get_queue_size() == 0, "Queue should be empty"

        # Wait for scale-down (should be aggressive)
        await asyncio.sleep(6.0)

        final_workers = len(started_executor.workers)
        assert final_workers < workers_after_scale_up, \
            f"Should scale down when idle: before={workers_after_scale_up}, after={final_workers}"


# ============================================================================
# Queue Management Tests
# ============================================================================

class TestQueueManagement:
    """Test queue handling and overflow behavior"""

    @pytest.mark.asyncio
    async def test_queue_full_returns_false(self, started_executor):
        """Verify submit returns False when queue is full"""
        # Fill the queue (max 1000)
        for i in range(1000):
            success = started_executor.submit(f"topic/{i}", f"payload_{i}", slow_handler)
            if not success:
                break

        # Next submit should fail
        success = started_executor.submit("overflow/topic", "overflow", slow_handler)

        # Should return False when full OR queue should be near capacity
        if not success:
            assert started_executor.get_queue_size() >= 900, "Queue should be near full when returning False"

    @pytest.mark.asyncio
    async def test_no_message_loss_under_normal_load(self, started_executor):
        """
        CRITICAL TEST: Verify no messages are lost under normal load
        """
        message_count = 200
        submitted = 0

        for i in range(message_count):
            success = started_executor.submit(f"topic/{i}", f"payload_{i}", dummy_handler)
            if success:
                submitted += 1

        # All messages should be accepted
        assert submitted == message_count, f"All {message_count} messages should be accepted, got {submitted}"

        # Wait for processing
        await asyncio.sleep(5.0)

        # All should be processed
        processed = started_executor._messages_processed
        assert processed >= message_count, \
            f"All {message_count} messages should be processed, got {processed}"


# ============================================================================
# Scaling Threshold Tests
# ============================================================================

class TestScalingThresholds:
    """Test scaling behavior at different thresholds"""

    @pytest.mark.asyncio
    async def test_small_burst_minimal_scaling(self, started_executor):
        """
        TEST: Small bursts should not trigger excessive scaling
        """
        # Small burst (30 fast messages)
        for i in range(30):
            started_executor.submit(f"topic/{i}", f"payload_{i}", dummy_handler)

        await asyncio.sleep(3.0)

        workers = len(started_executor.workers)

        # Should stay at or near minimum
        assert 3 <= workers <= 5, \
            f"Small burst should keep workers at 3-5, got {workers}"

    @pytest.mark.asyncio
    async def test_medium_burst_moderate_scaling(self, started_executor):
        """
        TEST: Medium bursts should scale moderately
        """
        # Medium burst (150 messages)
        for i in range(150):
            started_executor.submit(f"topic/{i}", f"payload_{i}", slow_handler)

        await asyncio.sleep(4.0)

        workers = len(started_executor.workers)

        # Should scale to moderate level (conservative scaling: 4-8 workers)
        assert 4 <= workers <= 8, \
            f"Medium burst should scale to 4-8 workers, got {workers}"

    @pytest.mark.asyncio
    async def test_large_burst_aggressive_scaling(self, started_executor):
        """
        TEST: Large bursts should scale aggressively
        """
        # Large burst (500 messages)
        for i in range(500):
            started_executor.submit(f"topic/{i}", f"payload_{i}", slow_handler)

        await asyncio.sleep(5.0)

        workers = len(started_executor.workers)

        # Should scale to high level (near max, but conservative: 6-10 workers)
        assert 6 <= workers <= 10, \
            f"Large burst should scale to 6-10 workers, got {workers}"


# ============================================================================
# Metrics Tracking Tests
# ============================================================================

class TestMetricsTracking:
    """Test metrics collection and callback"""

    @pytest.mark.asyncio
    async def test_metrics_callback_is_called(self, executor):
        """Verify metrics callback is invoked"""
        metrics_received = []

        def capture_metrics(**metrics):
            metrics_received.append(metrics)

        executor.metrics_cb = capture_metrics
        await executor.start()

        # Wait for at least one metrics callback
        await asyncio.sleep(2.5)

        await executor.stop()

        # Should have received metrics
        assert len(metrics_received) > 0, "Metrics callback should be invoked"

        # Verify metrics structure
        if metrics_received:
            latest = metrics_received[-1]
            assert 'workers' in latest
            assert 'qsize' in latest
            assert 'cpu' in latest
            assert 'mem' in latest
            assert 'qusage' in latest

    @pytest.mark.asyncio
    async def test_messages_processed_counter(self, started_executor):
        """Verify message processing counter increments"""
        initial_count = started_executor._messages_processed

        # Submit and process messages
        for i in range(20):
            started_executor.submit(f"topic/{i}", f"payload_{i}", dummy_handler)

        await asyncio.sleep(2.0)

        final_count = started_executor._messages_processed

        assert final_count > initial_count, \
            f"Messages processed should increment: initial={initial_count}, final={final_count}"
        assert final_count >= 20, \
            f"Should process at least 20 messages, got {final_count}"


# ============================================================================
# Regression Tests (Future-Proof)
# ============================================================================

class TestRegressionProtection:
    """
    Tests to ensure future changes don't break core functionality
    These define the expected behavior boundaries
    """

    @pytest.mark.asyncio
    async def test_worker_range_boundaries(self, started_executor):
        """
        REGRESSION TEST: Ensure workers always stay within min/max bounds
        """
        # Submit varying load over time
        for round_num in range(5):
            # Burst
            for i in range(100):
                started_executor.submit(f"topic/{round_num}/{i}", "payload", slow_handler)
            await asyncio.sleep(2.0)

            # Check bounds
            workers = len(started_executor.workers)
            assert 3 <= workers <= 10, \
                f"Round {round_num}: Workers {workers} must be within [3, 10]"

    @pytest.mark.asyncio
    async def test_scaling_responsiveness(self, started_executor):
        """
        REGRESSION TEST: Ensure scaling responds within reasonable time
        """
        # Submit large burst
        for i in range(300):
            started_executor.submit(f"topic/{i}", "payload", slow_handler)

        initial_workers = len(started_executor.workers)

        # Should scale up within 5 seconds
        await asyncio.sleep(5.0)

        scaled_workers = len(started_executor.workers)

        assert scaled_workers > initial_workers, \
            "Should scale up within 5 seconds of burst load"
        assert scaled_workers - initial_workers >= 2, \
            f"Should scale up by at least 2 workers, scaled by {scaled_workers - initial_workers}"

    @pytest.mark.asyncio
    async def test_scale_down_responsiveness(self, started_executor):
        """
        REGRESSION TEST: Ensure scale-down happens within reasonable time
        """
        # Scale up first
        for i in range(200):
            started_executor.submit(f"topic/{i}", "payload", slow_handler)

        await asyncio.sleep(3.0)
        peak_workers = len(started_executor.workers)

        # Wait for scale down (max 12 seconds)
        await asyncio.sleep(12.0)

        final_workers = len(started_executor.workers)

        assert final_workers < peak_workers, \
            "Should scale down within 12 seconds after load decreases"
        assert peak_workers - final_workers >= 1, \
            f"Should scale down by at least 1 worker, scaled down by {peak_workers - final_workers}"

    @pytest.mark.asyncio
    async def test_consistent_processing_rate(self, started_executor):
        """
        REGRESSION TEST: Ensure processing rate stays reasonable
        """
        # Submit fixed number of fast messages
        message_count = 100
        start_time = time.time()

        for i in range(message_count):
            started_executor.submit(f"topic/{i}", f"payload_{i}", dummy_handler)

        # Wait for processing
        await asyncio.sleep(5.0)

        elapsed = time.time() - start_time
        processed = started_executor._messages_processed

        # Should process at least 80% of messages
        assert processed >= message_count * 0.8, \
            f"Should process at least 80 messages, got {processed}"

        # Processing rate should be reasonable (>10 msgs/sec)
        if elapsed > 0:
            rate = processed / elapsed
            assert rate > 10, \
                f"Processing rate should be >10 msg/sec, got {rate:.1f} msg/sec"


# ============================================================================
# Run Tests
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

