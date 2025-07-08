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
    await process_manager.acquire(key)
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
