"""
Redis Pub/Sub listener for fraud alerts.

Subscribes to the 'fraud_alerts' channel and distributes
messages to connected SSE clients via asyncio queues.
"""
import asyncio
import json
from typing import Set

import redis.asyncio as aioredis


class RedisAlertListener:
    """
    Async Redis subscriber that fans out messages to multiple SSE clients.

    Each SSE client gets its own asyncio.Queue via subscribe().
    Messages from Redis are broadcast to all queues.
    """

    def __init__(self, host: str, port: int, channel: str):
        self.host = host
        self.port = port
        self.channel = channel
        self._subscribers: Set[asyncio.Queue] = set()
        self._running = False
        self._task = None

    def subscribe(self) -> asyncio.Queue:
        """Register a new SSE client. Returns a queue to read from."""
        queue = asyncio.Queue(maxsize=100)
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue):
        """Remove an SSE client."""
        self._subscribers.discard(queue)

    async def start(self):
        """Start listening to Redis Pub/Sub."""
        self._running = True
        while self._running:
            try:
                conn = aioredis.Redis(
                    host=self.host, port=self.port,
                    decode_responses=True,
                )
                pubsub = conn.pubsub()
                await pubsub.subscribe(self.channel)
                print(
                    f"📡 Subscribed to Redis channel: {self.channel}"
                )

                async for message in pubsub.listen():
                    if not self._running:
                        break
                    if message["type"] != "message":
                        continue

                    data = message["data"]

                    # Broadcast to all SSE clients
                    dead_queues = set()
                    for queue in self._subscribers:
                        try:
                            queue.put_nowait(data)
                        except asyncio.QueueFull:
                            dead_queues.add(queue)

                    # Clean up disconnected clients
                    self._subscribers -= dead_queues

                await pubsub.unsubscribe(self.channel)
                await conn.close()

            except Exception as e:
                if self._running:
                    print(f"⚠️ Redis listener error: {e}. Retrying in 5s...")
                    await asyncio.sleep(5)

    async def stop(self):
        """Stop the listener."""
        self._running = False
        # Unblock all waiting queues
        for queue in self._subscribers:
            try:
                queue.put_nowait(
                    json.dumps({"event": "shutdown"})
                )
            except asyncio.QueueFull:
                pass
        self._subscribers.clear()
