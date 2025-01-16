# Async Batch Processing Scheduler

This project provides an asynchronous batch processing system using Python's `asyncio`. It includes a custom `IterationScheduler` class that manages requests, processes them in batches, and integrates with a FastAPI backend for real-time API interaction.

## Features

- **Batch Processing**: Groups incoming requests into batches to optimize processing efficiency.
- **Prioritization**: Processes requests based on priority using an `asyncio.PriorityQueue`.
- **Timeout Handling**: Automatically removes requests that exceed their allowed processing time.
- **FastAPI Integration**: Exposes a simple API to submit and process requests.

---

## Requirements

- Python 3.8+
- FastAPI
- Uvicorn

Install dependencies with:
```bash
pip install fastapi uvicorn
