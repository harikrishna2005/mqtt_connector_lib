# self.r = redis.Redis(
#     host=host,
#     port=port,
#     password=password,
#     decode_responses=True,
#     protocol=3,  # Uses the high-performance RESP3 protocol (Redis 6.2+)
#     health_check_interval=30 # Detect dead sockets every 30s
# )

import asyncio
import redis.asyncio as redis
from redis.utils import HIREDIS_AVAILABLE
import hiredis


async def check_final():
    print(f"--- Environment Check ---")
    # print(f"Hiredis Version: {hiredis.__version__}")
    # print(f"Does Redis library see Hiredis?: {HIREDIS_AVAILABLE}")

    r = redis.Redis(
        host="divyaharihost.local",
        port=6379,
        password="SuperDuperRedis6748@",
        protocol=3  # RESP3 is faster for trading bots
    )

    conn = await r.connection_pool.get_connection('PING')
    parser = conn._parser.__class__.__name__
    print(f"Active Parser: {parser}")

    if "Hiredis" in parser:
        print("🚀 SUCCESS: You are using the high-performance C-parser.")
    else:
        print("❌ STILL USING PYTHON PARSER. Check 'poetry show hiredis'.")

    await r.connection_pool.release(conn)
    await r.aclose()


asyncio.run(check_final())