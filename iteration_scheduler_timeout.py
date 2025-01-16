import asyncio
import uuid
from typing import List, Dict, Callable, Optional

class Request:
    def __init__(self, request_id: str, data: dict):
        """
        Represents a single request.

        :param request_id: Unique identifier for the request.
        :param data: Request data payload.
        """
        self.request_id = request_id
        self.data = data
        self.tokens = 0  # Tracks the number of tokens processed for this request.
        self.done = False  # Indicates whether the request is fully processed.
        self.timestamp = asyncio.get_event_loop().time()  # Timestamp of when the request was created.

    def __lt__(self, other):
        """
        Comparison method used by PriorityQueue to sort requests.
        Requests with fewer tokens are given higher priority.
        """
        return self.tokens < other.tokens


class IterationScheduler:
    def __init__(self, batch_size: int, process_function: Callable, timeout: float, task_timeout: int):
        """
        Scheduler for managing and processing requests in batches.

        :param batch_size: Maximum number of requests in a single batch.
        :param process_function: Function to process a batch of requests.
        :param timeout: Maximum wait time for requests in the queue.
        """
        self.batch_size = batch_size
        self.process_function = process_function
        self.timeout = timeout
        self.task_timeout = task_timeout
        self.request_pool = asyncio.PriorityQueue()  # Queue to hold requests with priorities.
        self.response_dict: Dict[str, asyncio.Future] = {}  # Mapping of request_id to asyncio.Future for response handling.

    async def start(self):
        """
        Starts the batch processor in a background task.
        """
        self.batch_task = asyncio.create_task(self._batch_processor())

    async def add_request(self, request_id: str, data: dict) -> dict:
        """
        Adds a new request to the queue and waits for its result.

        :param request_id: Unique identifier for the request.
        :param data: Request data payload, including priority.
        :return: Processed result of the request.
        """
        priority = int(data["priority"])  # Extract priority from the data.
        future = asyncio.Future()  # Create a Future to hold the response.
        self.response_dict[request_id] = future

        # Create and add the request to the priority queue.
        req = Request(request_id=request_id, data=data)
        await self.request_pool.put((priority, req))
        return await future  # Wait for the response.

    async def _batch_processor(self):
        """
        Continuously processes requests in batches, respecting timeouts and priorities.
        """
        while True:
            try:
                batch = []  # List to hold the current batch of requests.
                futures = []  # List to track futures for the batch.
                current_time = asyncio.get_event_loop().time()  # Current monotonic time.

                while len(batch) < self.batch_size:
                    try:
                        # Retrieve a request from the priority queue with a timeout.
                        priority, request = await asyncio.wait_for(
                            self.request_pool.get(), timeout=self.timeout
                        )

                        # Discard requests that have exceeded their timeout.
                        if current_time - request.timestamp > self.task_timeout:  # seconds
                            future = self.response_dict.get(request.request_id)
                            if future and not future.done():
                                future.set_exception(
                                    TimeoutError("Request timed out while in queue.")
                                )
                            continue  # Skip this request.

                        # Add valid requests to the batch.
                        batch.append([priority, request])
                        futures.append(self.response_dict[request.request_id])
                    except asyncio.TimeoutError:
                        break  # Exit if no new requests are available.

                if not batch:
                    continue  # Skip processing if the batch is empty.

                # Process the batch using the execution engine.
                priorities = [x[0] for x in batch]
                responses = await self.process_function([x[1] for x in batch])

                # Handle responses and update futures or re-queue incomplete requests.
                for future, priority, response in zip(futures, priorities, responses):
                    if response.done:
                        if not future.done():
                            future.set_result(response)
                    else:
                        await self.request_pool.put((priority, response))  # Re-add incomplete requests.

            except Exception as e:
                # Handle any errors and update all affected futures with the exception.
                for future in futures:
                    if not future.done():
                        future.set_exception(e)

    async def shutdown(self):
        """
        Gracefully shuts down the scheduler by canceling the batch processor task.
        """
        self.batch_task.cancel()
        try:
            await self.batch_task
        except asyncio.CancelledError:
            pass


# Example Execution Engine
async def example_execution_engine(batch: List) -> List:
    """
    Simulates batch processing for the execution engine.
    Returns responses with updated tokens and completion status.

    :param batch: List of requests to process.
    :return: List of processed requests.
    """
    await asyncio.sleep(1)  # Simulated processing time.
    responses = []
    for request in batch:
        if request.tokens >= 5:  # Mark the request as complete after 5 tokens.
            request.done = True
        else:
            request.tokens += 1  # Increment tokens for the request.
            request.done = False
        responses.append(request)
    return responses


# Example Usage
if __name__ == "__main__":
    import uvicorn
    from fastapi import FastAPI

    app = FastAPI()

    # Initialize the scheduler with a batch size, processing function, and timeout.
    scheduler = IterationScheduler(
        batch_size=3,
        process_function=example_execution_engine,
        timeout=2.0,
        task_timeout=600
    )

    @app.post("/process")
    async def process_request(input_data: dict):
        """
        API endpoint to process a new request.

        :param input_data: Request data payload.
        :return: Processed result.
        """
        request_id = str(uuid.uuid4())
        result = await scheduler.add_request(request_id, input_data)
        return result

    @app.on_event("shutdown")
    async def shutdown_event():
        """
        Event triggered during application shutdown to clean up resources.
        """
        await scheduler.shutdown()

    @app.on_event("startup")
    async def on_startup():
        """
        Event triggered during application startup to initialize resources.
        """
        await scheduler.start()

    # Start the FastAPI server with Uvicorn.
    uvicorn.run(app, host="0.0.0.0", port=8000)