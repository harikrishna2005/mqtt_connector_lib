# 🧪 Quick Test Reference Card

## ⚡ Run Tests

```bash
# All tests (~60-90s)
poetry run pytest test_smart_scaling_executor_functional.py -v

# Critical tests only (~30s)
poetry run pytest test_smart_scaling_executor_functional.py::TestScaleUp::test_scales_up_on_burst_load test_smart_scaling_executor_functional.py::TestScaleDown::test_scales_down_when_idle -v

# Specific category
poetry run pytest test_smart_scaling_executor_functional.py::TestScaleUp -v
```

---

## ✅ What Tests Validate

| Test | Validates | Expected Result |
|------|-----------|-----------------|
| **test_scales_up_on_burst_load** | Workers increase under load | 3 → 5-8 workers |
| **test_scales_down_when_idle** | Workers decrease when idle | Peak → 3-6 workers |
| **test_no_message_loss** | All messages processed | 100% success |
| **test_worker_range_boundaries** | Min/max respected | 3 ≤ workers ≤ 10 |

---

## 📊 Acceptable Ranges

```
Idle:         3-5 workers
Small load:   3-5 workers (30 msgs)
Medium load:  5-8 workers (150 msgs)
Large load:   7-10 workers (500 msgs)

ABSOLUTE: 3 ≤ workers ≤ 10 always
```

---

## 🚨 When Tests Fail

**Tests SHOULD fail if you:**
- ❌ Break scale-up logic
- ❌ Break scale-down logic
- ❌ Violate min/max boundaries
- ❌ Cause message drops

**Tests MAY need update if you:**
- ✅ Improve scaling algorithm (update ranges)
- ✅ Change default min/max (update tests)
- ✅ Optimize performance (update timing)

---

## 📝 After Making Changes

```bash
# 1. Run tests
poetry run pytest test_smart_scaling_executor_functional.py -v

# 2. If tests fail:
#    - Review which test failed
#    - Check if intentional improvement
#    - Update acceptable ranges if justified

# 3. Integration test
poetry run python test_prometheus_demo.py

# 4. Check metrics
cat metrics.csv
```

---

## 📚 Test Categories

```python
TestBasicFunctionality      # Start/stop, submit, process
TestScaleUp                 # Scale-up behavior (CRITICAL)
TestScaleDown               # Scale-down behavior (CRITICAL)
TestQueueManagement         # Queue handling, no drops
TestScalingThresholds       # Burst size responses
TestMetricsTracking         # Metrics callback
TestRegressionProtection    # Future-proof boundaries
```

---

## 🎯 Test Files

- **test_smart_scaling_executor_functional.py** - Test suite (23 tests)
- **TEST_DOCUMENTATION.md** - Complete guide
- **smart_scaling_executor.py** - Code under test

---

**Quick validation:** `pytest test_smart_scaling_executor_functional.py -v`

