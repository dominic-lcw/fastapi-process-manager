# FastAPI Process Manager

This project provides a FastAPI application with endpoints to stream the progress of long-running tasks. It features a process manager that prevents concurrent execution of the same endpoint.

## Features

- Stream task progress using Server-Sent Events (SSE)
- Prevent concurrent runs of the same endpoint using a process manager

## Getting Started

1. Install dependencies:
   ```sh
   /Users/dominicleung/fastapi-process-manager/.venv/bin/python -m pip install fastapi uvicorn
   ```
2. Run the server:
   ```sh
   /Users/dominicleung/fastapi-process-manager/.venv/bin/python -m uvicorn main:app --reload
   ```
3. Visit `http://localhost:8000/stream-task` to test streaming.

## Project Structure

- `main.py`: FastAPI app, endpoints, and process manager

---

Replace `/Users/dominicleung/fastapi-process-manager/.venv/bin/python` with your Python path if different.
