import asyncio
import uuid
from typing import List, Dict, Callable, Optional


class Request:
    def __init__(self, request_id: str, data: dict):
        """
        Represents a request to be processed.

        :param request_id: Unique identifier for the request.
        :param data: The data associated with the request.
        """
        self.request_id = request_id  # Unique ID for the request
        self.data = data  # Request data
        self.tokens = 0  # Tracks processing progress
        self.done = False  # Indicates if the request is complete

    def __lt__(self, other):
        """
        Custom less-than method for sorting in PriorityQueue.
        Requests with fewer tokens are prioritized.
        """
        return self.tokens < other.tokens


class IterationScheduler:
    def __init__(self, batch_size: int, process_function: Callable, timeout: float):
        """
        A scheduler that batches and processes requests iteratively.

        :param batch_size: Maximum number of requests in a batch.
        :param process_function: Function to process batches of requests.
        :param timeout: Maximum time to wait for a new request.
        """
        self.batch_size = batch_size  # Max batch size
        self.process_function = process_function  # Function to process requests
        self.timeout = timeout  # Timeout for waiting for requests

        self.request_pool = asyncio.PriorityQueue()  # Priority queue for request scheduling
        self.response_dict: Dict[str, asyncio.Future] = {}  # Maps request IDs to their Future objects

    async def start(self):
        """
        Starts the scheduler by initializing the batch processor task.
        """
        self.batch_task = asyncio.create_task(self._batch_processor())

    async def add_request(self, request_id: str, data: dict) -> dict:
        """
        Adds a new request to the scheduler and waits for the result.

        :param request_id: Unique identifier for the request.
        :param data: Data to be processed.
        :return: Processed result.
        """
        priority = int(data["priority"])  # Priority of the request
        future = asyncio.Future()  # Future to hold the result
        self.response_dict[request_id] = future  # Map request ID to its Future

        # Add request to the pool with priority
        req = Request(request_id=request_id, data=data)
        await self.request_pool.put((priority, req))
        return await future  # Wait for the result

    async def _batch_processor(self):
        """
        Processes requests in batches, sending them to the execution engine.
        Handles incomplete requests by re-queuing them.
        """
        while True:
            try:
                batch = []  # Current batch of requests
                futures = []  # Corresponding futures for the batch

                while len(batch) < self.batch_size:
                    try:
                        # Retrieve a request from the pool within the timeout
                        priority, request = await asyncio.wait_for(
                            self.request_pool.get(), timeout=self.timeout
                        )
                        batch.append([priority, request])  # Add to batch
                        futures.append(self.response_dict[request.request_id])  # Track future
                    except asyncio.TimeoutError:
                        break  # Stop if no requests are available

                if not batch:
                    continue  # Skip if no batch was formed

                # Process the batch using the execution engine
                priorities = [x[0] for x in batch]
                responses = await self.process_function([x[1] for x in batch])

                # Handle the responses
                for future, priority, response in zip(futures, priorities, responses):
                    if response.done:  # If processing is complete
                        if not future.done():
                            future.set_result(response)
                    else:  # Re-queue incomplete requests
                        await self.request_pool.put((priority, response))

            except Exception as e:
                # Handle exceptions and set them in pending futures
                for future in futures:
                    if not future.done():
                        future.set_exception(e)

    async def shutdown(self):
        """
        Shuts down the scheduler gracefully by canceling the batch processor task.
        """
        self.batch_task.cancel()
        try:
            await self.batch_task
        except asyncio.CancelledError:
            pass


# Example Execution Engine
async def example_execution_engine(batch: List) -> List:
    """
    Simulates processing a batch of requests.

    :param batch: List of requests to process.
    :return: List of processed requests with updated tokens and completion status.
    """
    await asyncio.sleep(1)  # Simulated processing delay
    responses = []
    for request in batch:
        if request.tokens >= 5:  # Mark as complete if tokens >= 5
            request.done = True
        else:  # Increment tokens for incomplete requests
            request.tokens += 1
            request.done = False
        responses.append(request)
    return responses


# Example Usage
if __name__ == "__main__":
    import uvicorn
    from fastapi import FastAPI

    app = FastAPI()

    # Initialize the scheduler
    scheduler = IterationScheduler(
        batch_size=3,
        process_function=example_execution_engine,
        timeout=2.0,
    )

    @app.post("/process")
    async def process_request(input_data: dict):
        """
        Endpoint to handle incoming requests and return processed results.

        :param input_data: Request payload containing data and priority.
        :return: Processed result from the scheduler.
        """
        request_id = str(uuid.uuid4())  # Generate a unique request ID
        result = await scheduler.add_request(request_id, input_data)
        return result

    @app.on_event("shutdown")
    async def shutdown_event():
        """
        Event triggered on application shutdown to clean up resources.
        """
        await scheduler.shutdown()

    @app.on_event("startup")
    async def on_startup():
        """
        Event triggered on application startup to initialize the scheduler.
        """
        await scheduler.start()

    # Run the application
    uvicorn.run(app, host="0.0.0.0", port=8000)
