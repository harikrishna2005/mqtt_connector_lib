# pip install opentelemetry-sdk opentelemetry-api opentelemetry-exporter-otlp opentelemetry-instrumentation-logging
import time
import requests
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource

# ======================================================
# 1. CONFIGURATION
# ======================================================
SERVICE_ID = "remote-python-loki"
# Use your host address (divyaharihost.local)
ALLOY_HOST = "divyaharihost.local"
OTLP_EXPORTER_ENDPOINT = f"http://{ALLOY_HOST}:4317"
LOKI_CUSTOM_API_ENDPOINT = f"http://{ALLOY_HOST}:9999/loki/api/v1/push"

# ======================================================
# 2. SETUP OPENTELEMETRY (TEMPO PATH)
# ======================================================
resource = Resource(attributes={
    SERVICE_NAME: SERVICE_ID
})

provider = TracerProvider(resource=resource)
# The BatchSpanProcessor runs in a background thread
processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=OTLP_EXPORTER_ENDPOINT, insecure=True))
provider.add_span_processor(processor)
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(__name__)

# ======================================================
# 3. EXECUTION
# ======================================================
print(f"Starting telemetry for {SERVICE_ID}...")

with tracer.start_as_current_span("manual-execution-span") as span:
    ctx = span.get_span_context()
    trace_id = format(ctx.trace_id, '032x')
    span_id = format(ctx.span_id, '016x')

    print(f"Generated Trace ID: {trace_id}")
    print(f"Generated Span ID:  {span_id}")

    # Simulate work
    time.sleep(1)

    # 4. SEND LOG TO LOKI (LOKI PATH)
    log_message = f"trace_id={trace_id} span_id={span_id} resource.service.name={SERVICE_ID} message='Hello from fixed script'"

    loki_payload = {
        "streams": [{
            "stream": {"service_name": SERVICE_ID},
            "values": [[str(time.time_ns()), log_message]]
        }]
    }

    try:
        resp = requests.post(LOKI_CUSTOM_API_ENDPOINT, json=loki_payload)
        resp.raise_for_status()
        print("Successfully sent log to Loki")
    except Exception as e:
        print(f"Failed to send log to Loki: {e}")

# ======================================================
# 4. THE CRITICAL FIX: SHUTDOWN
# ======================================================
print("Cleaning up and flushing traces...")

# This forces the background exporter to finish sending data to Alloy [cite: 12, 13]

provider.shutdown()

print("Done. You should no longer see the UNAVAILABLE error.")