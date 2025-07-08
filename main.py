import asyncio
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from typing import AsyncGenerator
from api.manager import ProcessManager

app = FastAPI()

process_manager = ProcessManager()


async def long_running_task() -> AsyncGenerator[str, None]:
    for i in range(10):
        await asyncio.sleep(1)
        yield f"data: Step {i + 1}/10 completed\n"
    yield "data: Task finished!\n"


async def waiting_event_generator(key: str):
    yield "data: Task is already running, waiting for result...\n"
    cached_result = await process_manager.get_cached_result(key, wait=True)
    if cached_result is not None:
        for message in cached_result:
            yield message
    else:
        yield "data: Task already running and no cached result.\n"


@app.get("/stream-task")
async def stream_task(request: Request):
    key = "stream-task"
    if key in process_manager.locks and process_manager.locks[key].locked():
        return StreamingResponse(
            waiting_event_generator(key), media_type="text/event-stream"
        )

    # Use timeout for lock acquisition
    acquired = await process_manager.acquire(key, timeout=30.0)
    if not acquired:

        async def timeout_generator():
            yield "data: Server busy, please try again later\n"

        return StreamingResponse(timeout_generator(), media_type="text/event-stream")

    result = []

    async def event_generator():
        try:
            async for message in long_running_task():
                result.append(message)
                yield message
                if await request.is_disconnected():
                    break
        finally:
            process_manager.release(key, result)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.get("/stream-task-managed")
async def stream_task_managed(request: Request):
    """Alternative endpoint using context manager for automatic lock management."""
    key = "stream-task-managed"

    # Check if already running
    if key in process_manager.locks and process_manager.locks[key].locked():
        return StreamingResponse(
            waiting_event_generator(key), media_type="text/event-stream"
        )

    result = []

    async def managed_event_generator():
        try:
            async with process_manager.managed_lock(key, timeout=30.0):
                async for message in long_running_task():
                    result.append(message)
                    yield message
                    if await request.is_disconnected():
                        break
                # Store result after completion
                process_manager.cache[key] = result
                if key in process_manager.cache_events:
                    process_manager.cache_events[key].set()
        except asyncio.TimeoutError:
            yield "data: Server busy, please try again later\n"

    return StreamingResponse(managed_event_generator(), media_type="text/event-stream")


@app.get("/stats")
async def get_manager_stats():
    """Get ProcessManager statistics for monitoring."""
    return process_manager.get_stats()
