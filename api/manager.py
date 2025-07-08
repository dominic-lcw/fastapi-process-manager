import asyncio
import time
from typing import Dict, Any, Optional, Set
from contextlib import asynccontextmanager
from cachetools import LRUCache
import logging

logger = logging.getLogger(__name__)


class ProcessManager:
    def __init__(
        self,
        cache_size: int = 100,
        lock_timeout: float = 300.0,
        cleanup_interval: float = 600.0,
    ):
        self.cache_size = cache_size
        self.lock_timeout = lock_timeout
        self.cleanup_interval = cleanup_interval

        # Core data structures
        self.locks: Dict[str, asyncio.Lock] = {}
        self.cache: LRUCache = LRUCache(maxsize=cache_size)
        self.cache_events: Dict[str, asyncio.Event] = {}

        # Tracking and cleanup
        self.lock_creation_lock = asyncio.Lock()
        self.active_keys: Set[str] = set()
        self.last_access: Dict[str, float] = {}

        # Start cleanup task
        self._cleanup_task = asyncio.create_task(self._periodic_cleanup())

    async def _get_or_create_lock(self, key: str) -> asyncio.Lock:
        """Thread-safe lock creation to prevent race conditions."""
        if key not in self.locks:
            async with self.lock_creation_lock:
                if key not in self.locks:  # Double-check pattern
                    self.locks[key] = asyncio.Lock()
                    self.cache_events[key] = asyncio.Event()
                    logger.debug(f"Created new lock for key: {key}")

        self.last_access[key] = time.time()
        return self.locks[key]

    async def acquire(self, key: str, timeout: Optional[float] = None) -> bool:
        """Acquire lock with optional timeout."""
        lock = await self._get_or_create_lock(key)

        try:
            timeout = timeout or self.lock_timeout
            await asyncio.wait_for(lock.acquire(), timeout=timeout)
            self.active_keys.add(key)
            # Reset event for new run
            if key in self.cache_events:
                self.cache_events[key].clear()
            logger.debug(f"Acquired lock for key: {key}")
            return True
        except asyncio.TimeoutError:
            logger.warning(f"Timeout acquiring lock for key: {key}")
            return False

    def release(self, key: str, result: Any = None):
        """Release lock and optionally cache result."""
        try:
            if key in self.locks and self.locks[key].locked():
                self.locks[key].release()
                self.active_keys.discard(key)
                logger.debug(f"Released lock for key: {key}")

            if result is not None:
                self.cache[key] = result
                if key in self.cache_events:
                    self.cache_events[key].set()
                    logger.debug(f"Cached result and set event for key: {key}")
        except Exception as e:
            logger.error(f"Error releasing lock for key {key}: {e}")

    async def get_cached_result(
        self, key: str, wait: bool = True, timeout: Optional[float] = None
    ) -> Optional[Any]:
        """Get cached result, optionally waiting for completion."""
        if key in self.locks and self.locks[key].locked():
            if wait and key in self.cache_events:
                try:
                    timeout = timeout or self.lock_timeout
                    await asyncio.wait_for(
                        self.cache_events[key].wait(), timeout=timeout
                    )
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout waiting for cached result for key: {key}")
                    return None

        result = self.cache.get(key)
        if result is not None:
            self.last_access[key] = time.time()
        return result

    @asynccontextmanager
    async def managed_lock(self, key: str, timeout: Optional[float] = None):
        """Context manager for automatic lock management."""
        acquired = await self.acquire(key, timeout)
        if not acquired:
            raise asyncio.TimeoutError(f"Could not acquire lock for key: {key}")

        try:
            yield key
        finally:
            self.release(key)

    async def _periodic_cleanup(self):
        """Periodic cleanup of unused locks and events."""
        while True:
            try:
                await asyncio.sleep(self.cleanup_interval)
                await self._cleanup_unused_resources()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic cleanup: {e}")

    async def _cleanup_unused_resources(self):
        """Remove unused locks and events to prevent memory leaks."""
        current_time = time.time()
        keys_to_remove = []

        for key in list(self.locks.keys()):
            # Skip if currently active
            if key in self.active_keys:
                continue

            # Skip if recently accessed
            last_access = self.last_access.get(key, 0)
            if current_time - last_access < self.cleanup_interval:
                continue

            # Skip if lock is currently held
            if self.locks[key].locked():
                continue

            keys_to_remove.append(key)

        for key in keys_to_remove:
            self.locks.pop(key, None)
            self.cache_events.pop(key, None)
            self.last_access.pop(key, None)
            logger.debug(f"Cleaned up resources for unused key: {key}")

        if keys_to_remove:
            logger.info(f"Cleaned up {len(keys_to_remove)} unused resources")

    def get_stats(self) -> Dict[str, Any]:
        """Get manager statistics for monitoring."""
        return {
            "active_locks": len(self.active_keys),
            "total_locks": len(self.locks),
            "cache_size": len(self.cache),
            "cache_max_size": self.cache_size,
            "cache_hits": getattr(self.cache, "hits", 0),
            "cache_misses": getattr(self.cache, "misses", 0),
        }

    async def shutdown(self):
        """Clean shutdown of the manager."""
        if hasattr(self, "_cleanup_task"):
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

        # Release all locks
        for key in list(self.active_keys):
            self.release(key)

        logger.info("ProcessManager shutdown complete")
