import asyncio
import logging
from datetime import datetime
from typing import Optional
import redis.asyncio as redis

logger = logging.getLogger(__name__)


class RedisConnector:
    def __init__(
        self,
        host: str,
        port: int = 6379,
        password: Optional[str] = None,
        db: int = 0,
        health_check_interval: float = 5.0,
        circuit_breaker_threshold: int = 3,
        circuit_breaker_timeout: float = 30.0,
        stats_log_interval: float = 10.0,
        auto_start: bool = True
    ):
        """
        Initialize RedisConnector

        Args:
            host: Redis server hostname or IP address
            port: Redis server port (default: 6379)
            password: Redis password (optional)
            db: Redis database number (default: 0)
            health_check_interval: How often to check Redis health in seconds
            circuit_breaker_threshold: Number of failures before opening circuit
            circuit_breaker_timeout: Seconds to wait before retrying after circuit opens
            stats_log_interval: How often to log statistics in seconds
            auto_start: Automatically start health monitoring (default: True)
        """

        self.redis_client = redis.Redis(
            host=host,
            port=port,
            password=password,
            db=db,
            decode_responses=True,
            protocol=3,
            health_check_interval=30,
            socket_connect_timeout=5,  # Increased for network stability
            socket_timeout=5,  # Increased for high-load scenarios
            socket_keepalive=True,  # Keep connections alive
            socket_keepalive_options={},
            max_connections=50  # Increase connection pool size
        )

        # Circuit breaker state
        self._is_healthy = True
        self._consecutive_failures = 0
        self._health_check_interval = health_check_interval
        self._circuit_breaker_threshold = circuit_breaker_threshold
        self._circuit_breaker_timeout = circuit_breaker_timeout
        self._circuit_open_time = None

        # Statistics
        self._messages_sent = 0
        self._messages_dropped = 0
        self._messages_congested = 0
        self._last_stats_log = datetime.now()
        self._stats_log_interval = stats_log_interval

        # Background tasks
        self._health_check_task = None
        self._is_running = False
        self._auto_start = auto_start

        # Auto-start health monitoring if enabled
        if self._auto_start:
            self._start_monitoring()

    def _start_monitoring(self):
        """Start background health monitoring (called from __init__ or start())"""
        if not self._is_running:
            self._is_running = True
            self._health_check_task = asyncio.create_task(self._background_health_check())
            logger.info("✅ Redis connector started with health monitoring")

    async def start(self):
        """
        Start background health monitoring (manual control)
        Note: Health monitoring starts automatically by default unless auto_start=False
        """
        self._start_monitoring()

    async def stop(self):
        """Stop background health monitoring"""
        self._is_running = False
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass
        logger.info("🛑 Redis connector stopped")

    async def close(self):
        """Close Redis connection and stop monitoring"""
        await self.stop()
        await self.redis_client.aclose()

    async def _background_health_check(self):
        """Continuously check Redis health in background"""
        while self._is_running:
            try:
                await asyncio.sleep(self._health_check_interval)

                # Check if circuit breaker should reset
                if not self._is_healthy and self._circuit_open_time:
                    elapsed = (datetime.now() - self._circuit_open_time).total_seconds()
                    if elapsed >= self._circuit_breaker_timeout:
                        if await self._ping_redis():
                            self._is_healthy = True
                            self._consecutive_failures = 0
                            self._circuit_open_time = None
                            logger.info("✅ Redis connection recovered!")

                # Regular health check if circuit is closed
                elif self._is_healthy:
                    if not await self._ping_redis():
                        self._consecutive_failures += 1
                        if self._consecutive_failures >= self._circuit_breaker_threshold:
                            self._is_healthy = False
                            self._circuit_open_time = datetime.now()
                            logger.error(
                                f"⚠️ Circuit breaker OPENED after {self._consecutive_failures} failures"
                            )
                    else:
                        self._consecutive_failures = 0

                # Periodic stats logging
                self._log_stats_if_needed()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check error: {e}")

    def _log_stats_if_needed(self):
        """Log statistics periodically (not on every call!)"""
        now = datetime.now()
        elapsed = (now - self._last_stats_log).total_seconds()

        if elapsed >= self._stats_log_interval:
            total = self._messages_sent + self._messages_dropped + self._messages_congested
            if total > 0:
                success_rate = (self._messages_sent / total) * 100
                logger.info(
                    f"📊 Stats | Sent: {self._messages_sent} | "
                    f"Dropped: {self._messages_dropped} | "
                    f"Congested: {self._messages_congested} | "
                    f"Success: {success_rate:.1f}% | "
                    f"Health: {'✅' if self._is_healthy else '❌'}"
                )
            self._last_stats_log = now

    async def _ping_redis(self) -> bool:
        """Quick Redis ping with timeout"""
        try:
            await asyncio.wait_for(
                self.redis_client.ping(),
                timeout=2.0
            )
            return True
        except Exception:
            return False

    @property
    def is_healthy(self) -> bool:
        """Check if Redis connection is healthy"""
        return self._is_healthy

    def increment_sent(self, count: int):
        """Increment sent message counter"""
        self._messages_sent += count

    def increment_dropped(self, count: int):
        """Increment dropped message counter"""
        self._messages_dropped += count

    def increment_congested(self, count: int):
        """Increment congested message counter"""
        self._messages_congested += count

    def record_failure(self):
        """Record a failure and potentially trip circuit breaker"""
        self._consecutive_failures += 1
        if self._consecutive_failures >= self._circuit_breaker_threshold:
            self._is_healthy = False
            self._circuit_open_time = datetime.now()
            logger.error("⚠️ Circuit breaker OPENED")

    def reset_failures(self):
        """Reset consecutive failures counter"""
        self._consecutive_failures = 0

    def get_stats(self) -> dict:
        """Get connection statistics"""
        total = self._messages_sent + self._messages_dropped + self._messages_congested
        success_rate = (self._messages_sent / total * 100) if total > 0 else 0

        return {
            "messages_sent": self._messages_sent,
            "messages_dropped": self._messages_dropped,
            "messages_congested": self._messages_congested,
            "success_rate": f"{success_rate:.2f}%",
            "is_healthy": self._is_healthy,
            "consecutive_failures": self._consecutive_failures
        }


class HighSpeedPublisher:
    def __init__(
            self,
            redis_connector: RedisConnector,
            max_concurrent_tasks: int = 10
    ):
        """
        Initialize HighSpeedPublisher

        Args:
            redis_connector: RedisConnector instance
            max_concurrent_tasks: Maximum number of concurrent pipeline operations
        """
        self._redis_connector = redis_connector
        self._semaphore = asyncio.Semaphore(max_concurrent_tasks)

    async def publish_prices(self, price_dict) -> bool:
        """
        Publish prices with fast-fail on unhealthy Redis
        Returns: True if published, False if dropped

        **Silent operation** - logs only critical events
        """
        # Fast-fail if circuit breaker is open (no logging)
        if not self._redis_connector.is_healthy:
            self._redis_connector.increment_dropped(len(price_dict))
            return False

        # Fast-fail if semaphore locked (no logging)
        if self._semaphore.locked():
            self._redis_connector.increment_congested(len(price_dict))
            return False

        async with self._semaphore:
            try:
                await asyncio.wait_for(
                    self._execute_pipeline(price_dict),
                    timeout=3.0
                )
                self._redis_connector.increment_sent(len(price_dict))
                self._redis_connector.reset_failures()
                return True

            except asyncio.TimeoutError:
                self._redis_connector.increment_dropped(len(price_dict))
                self._redis_connector.record_failure()
                return False

            except (redis.ConnectionError, redis.TimeoutError):
                self._redis_connector.increment_dropped(len(price_dict))
                self._redis_connector.record_failure()
                return False

            except Exception as e:
                # Log unexpected errors (should be rare)
                self._redis_connector.increment_dropped(len(price_dict))
                logger.error(f"❌ Unexpected error while publising the prices: {e}")
                return False

    async def _execute_pipeline(self, price_dict):
        """Execute Redis pipeline with optimized batching"""
        async with self._redis_connector.redis_client.pipeline(transaction=False) as pipe:
            # Pre-format all commands to reduce overhead
            for pair, price in price_dict.items():
                pipe.publish(pair, str(price))
                # await pipe.publish(pair, str(price))
            # Single await for entire pipeline
            await pipe.execute()


class HighSpeedSubscriber:
    """
    High-speed Redis subscriber with automatic reconnection and re-subscription.

    Features:
    - Automatic reconnection on Redis disconnect
    - Automatic re-subscription to all channels after reconnect
    - Connection health monitoring
    - Handler registry preserved across reconnects
    - Direct handler execution (fast for simple price updates)

    Perfect for trading bots - subscribe to price channels and update class attributes.

    Usage:
        subscriber = HighSpeedSubscriber(redis_connector)
        subscriber.subscribe("BTCUSDT", self.update_btc_price)
        subscriber.subscribe("ETHUSDT", self.update_eth_price)
        await subscriber.start()
    """

    def __init__(
        self,
        redis_connector: RedisConnector,
        auto_reconnect: bool = True,
        reconnect_delay: float = 5.0,
        max_reconnect_attempts: int = 0  # 0 = infinite
    ):
        """
        Initialize HighSpeedSubscriber

        Args:
            redis_connector: RedisConnector instance
            auto_reconnect: Enable automatic reconnection (default: True)
            reconnect_delay: Delay between reconnection attempts in seconds (default: 5.0)
            max_reconnect_attempts: Maximum reconnection attempts, 0 for infinite (default: 0)
        """
        self._redis_connector = redis_connector
        self._pubsub = None
        self._handlers = {}  # {channel: handler_method} - preserved across reconnects
        self._is_running = False
        self._listen_task = None

        # Reconnection settings
        self._auto_reconnect = auto_reconnect
        self._reconnect_delay = reconnect_delay
        self._max_reconnect_attempts = max_reconnect_attempts
        self._reconnect_attempts = 0
        self._is_connected = False

    def subscribe(self, channel: str, handler_method):
        """
        Subscribe to a channel with a handler method.

        The handler method will be called with the message data when a message arrives.

        Args:
            channel: Channel name (e.g., "BTCUSDT", "ETHUSDT")
            handler_method: Callable that takes message data as argument

        Example:
            class TradingBot:
                def __init__(self):
                    self.btc_price = 0.0
                    self.eth_price = 0.0

                def update_btc_price(self, price_str):
                    self.btc_price = float(price_str)

                def update_eth_price(self, price_str):
                    self.eth_price = float(price_str)

            bot = TradingBot()
            subscriber.subscribe("BTCUSDT", bot.update_btc_price)
            subscriber.subscribe("ETHUSDT", bot.update_eth_price)
        """
        if not callable(handler_method):
            raise ValueError(f"Handler for channel '{channel}' must be callable")

        self._handlers[channel] = handler_method
        logger.info(f"📝 Registered handler for channel: {channel}")

    def unsubscribe(self, channel: str):
        """
        Unsubscribe from a channel.

        Args:
            channel: Channel name to unsubscribe from
        """
        if channel in self._handlers:
            del self._handlers[channel]
            logger.info(f"🗑️ Unregistered handler for channel: {channel}")

    async def start(self):
        """
        Start listening to subscribed channels with automatic reconnection.

        This will create a Redis PubSub connection and start listening for messages.
        Messages are dispatched to their registered handlers.

        If Redis disconnects, the subscriber will automatically reconnect and
        re-subscribe to all registered channels.
        """
        if self._is_running:
            logger.warning("⚠️ Subscriber is already running")
            return

        if not self._handlers:
            logger.warning("⚠️ No channels subscribed. Use subscribe() first.")
            return

        # Start main loop
        self._is_running = True
        self._listen_task = asyncio.create_task(self._main_loop())
        logger.info("🎧 Subscriber started with auto-reconnection enabled")

    async def stop(self):
        """
        Stop listening and close PubSub connection.
        """
        if not self._is_running:
            return

        self._is_running = False

        # Cancel listen task
        if self._listen_task:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass

        # Close PubSub connection
        await self._close_pubsub()

        logger.info("🛑 Subscriber stopped")

    async def _close_pubsub(self):
        """Close PubSub connection safely"""
        if self._pubsub:
            try:
                await self._pubsub.unsubscribe()
                await self._pubsub.aclose()
            except Exception as e:
                logger.warning(f"⚠️ Error closing PubSub: {e}")
            finally:
                self._pubsub = None
                self._is_connected = False

    async def _main_loop(self):
        """
        Main loop with automatic reconnection logic.
        """
        try:
            while self._is_running:
                try:
                    # Connect and subscribe
                    await self._connect_and_subscribe()

                    # Listen for messages
                    await self._listen_loop()

                except (redis.ConnectionError, redis.TimeoutError, ConnectionError) as e:
                    logger.error(f"❌ Connection lost: {e}")
                    self._is_connected = False

                    # Close current connection
                    await self._close_pubsub()

                    # Handle reconnection
                    if self._auto_reconnect and self._is_running:
                        await self._handle_reconnection()
                    else:
                        logger.error("❌ Auto-reconnect disabled, stopping subscriber")
                        break

                except Exception as e:
                    logger.error(f"❌ Unexpected error in main loop: {e}")
                    if self._is_running:
                        await asyncio.sleep(1)

        except asyncio.CancelledError:
            pass  # Clean shutdown

    async def _connect_and_subscribe(self):
        """
        Connect to Redis and subscribe to all registered channels.
        """
        if self._is_connected:
            return

        logger.info("🔌 Connecting to Redis PubSub...")

        # Create new PubSub instance
        self._pubsub = self._redis_connector.redis_client.pubsub()

        # Subscribe to all registered channels
        channels_list = list(self._handlers.keys())
        logger.info(f"📡 Re-subscribing to {len(channels_list)} channels...")

        for channel in channels_list:
            await self._pubsub.subscribe(channel)
            logger.info(f"✅ Subscribed to channel: {channel}")

        self._is_connected = True
        self._reconnect_attempts = 0  # Reset counter on successful connection
        logger.info("✅ Connected and subscribed to all channels")

    async def _handle_reconnection(self):
        """
        Handle reconnection with exponential backoff.
        """
        self._reconnect_attempts += 1

        # Check max attempts
        if self._max_reconnect_attempts > 0 and self._reconnect_attempts > self._max_reconnect_attempts:
            logger.error(f"❌ Max reconnection attempts ({self._max_reconnect_attempts}) reached. Stopping.")
            self._is_running = False
            return

        logger.warning(
            f"🔄 Reconnecting in {self._reconnect_delay}s "
            f"(attempt {self._reconnect_attempts}"
            f"{f'/{self._max_reconnect_attempts}' if self._max_reconnect_attempts > 0 else ''})"
        )

        await asyncio.sleep(self._reconnect_delay)

    async def _listen_loop(self):
        """
        Listen for messages and dispatch to handlers.
        """
        while self._is_running and self._is_connected:
            try:
                # Get message with timeout
                message = await asyncio.wait_for(
                    self._pubsub.get_message(ignore_subscribe_messages=True),
                    timeout=1.0
                )

                if message and message['type'] == 'message':
                    channel = message['channel']
                    data = message['data']

                    # Dispatch to handler
                    if channel in self._handlers:
                        handler = self._handlers[channel]
                        try:
                            # Call handler (supports both sync and async)
                            if asyncio.iscoroutinefunction(handler):
                                await handler(data)
                            else:
                                handler(data)
                        except Exception as e:
                            logger.error(f"❌ Error in handler for {channel}: {e}")

            except asyncio.TimeoutError:
                # No message received, continue loop
                continue
            except (redis.ConnectionError, redis.TimeoutError) as e:
                # Connection error - will be caught by main loop
                raise
            except Exception as e:
                if self._is_running:
                    logger.error(f"❌ Error processing message: {e}")
                    await asyncio.sleep(0.1)

    @property
    def is_running(self) -> bool:
        """Check if subscriber is currently running"""
        return self._is_running

    @property
    def is_connected(self) -> bool:
        """Check if subscriber is connected to Redis"""
        return self._is_connected

    def get_subscribed_channels(self) -> list:
        """Get list of currently subscribed channels"""
        return list(self._handlers.keys())

    def get_connection_info(self) -> dict:
        """
        Get connection status information.

        Returns:
            dict with keys:
                - is_running: bool
                - is_connected: bool
                - subscribed_channels: list
                - reconnect_attempts: int
                - auto_reconnect: bool
        """
        return {
            "is_running": self._is_running,
            "is_connected": self._is_connected,
            "subscribed_channels": list(self._handlers.keys()),
            "num_channels": len(self._handlers),
            "reconnect_attempts": self._reconnect_attempts,
            "auto_reconnect": self._auto_reconnect
        }


