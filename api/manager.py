import asyncio
from typing import Dict, Any, Optional


class ProcessManager:
    def __init__(self):
        self.locks: Dict[str, asyncio.Lock] = {}
        self.cache: Dict[str, Any] = {}
        self.cache_events: Dict[str, asyncio.Event] = {}

    async def acquire(self, key: str):
        if key not in self.locks:
            self.locks[key] = asyncio.Lock()
        if key not in self.cache_events:
            self.cache_events[key] = asyncio.Event()
        lock = self.locks[key]
        await lock.acquire()
        # Reset event for new run
        self.cache_events[key].clear()

    def release(self, key: str, result: Any = None):
        if key in self.locks and self.locks[key].locked():
            self.locks[key].release()
        if result is not None:
            self.cache[key] = result
            if key in self.cache_events:
                self.cache_events[key].set()

    async def get_cached_result(self, key: str, wait: bool = True) -> Optional[Any]:
        if key in self.locks and self.locks[key].locked():
            # Wait for cache event if requested
            if wait and key in self.cache_events:
                await self.cache_events[key].wait()
        return self.cache.get(key)
