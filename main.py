import asyncio
import numpy as np
from queue import Queue

class BatchProcessor:
    def __init__(self):
        self.request_queue = Queue()
        self.batch_size = 3  # Adjust the batch size according to your needs

    async def put_execution(self, query, future):
        self.request_queue.put({"data": query, "response": future})
    
    async def wait_for_capacity(self):
        while self.request_queue.qsize() < self.batch_size:
            await asyncio.sleep(0.5)  # Adjust the sleep time as needed

    async def process_batches(self):
        while True:
            # ожидаем либо секунду, либо пока очередь не будет больше batch_size
            await asyncio.wait(
                [
                    asyncio.create_task(self.wait_for_capacity()), 
                    asyncio.create_task(asyncio.sleep(1))
                ],
                return_when=asyncio.FIRST_COMPLETED
            )
            batch_size = min(self.request_queue.qsize(), self.batch_size)
            if batch_size == 0:
                continue
            
            future, batch_data = [], []
            for _ in range(batch_size):
                q_item = self.request_queue.get()
                future.append(q_item["response"])
                batch_data.append(q_item["data"])

            # Perform batch inference here (replace with your actual inference logic)
            # For demonstration, we'll use a simple sum of the input data
            batch_result = np.cumsum(batch_data)

            # Notify each individual requester with their processed result
            for res, item in zip(future, batch_result):
                res.set_result({"result": item})


# Глобальный объект для обработки последовательностей
batch_processor = BatchProcessor()

# Эмулятор входящих запросов
async def sleepy_request(request):
    sleep_time = int(np.random.rand(1) * 4) + 1
    # print(f"sleeping for {sleep_time}")
    await asyncio.sleep(sleep_time)
    response = asyncio.Future()
    await batch_processor.put_execution(request, response)
    result = await response
    return result

# воркеры, которые сабмитят входящие запросы с некоторой частотой
async def worker(worker_id, task_queue):
    while True:
        task = await task_queue.get()
        if task is None:
            # Signal to exit the worker when a None task is received
            break

        res = await sleepy_request(task)
        print(f"worker {worker_id} done {task} with res {res}")

        # Mark the task as done
        task_queue.task_done()

async def main():
    processor = asyncio.create_task(batch_processor.process_batches())
    # Number of worker threads
    num_workers = 50

    # Create an asyncio Queue for task distribution
    task_queue = asyncio.Queue()

    # Create and start worker tasks
    worker_tasks = []
    for worker_id in range(num_workers):
        task = asyncio.create_task(worker(worker_id, task_queue))
        worker_tasks.append(task)

    # Enqueue tasks
    tasks_to_process = np.arange(50)
    for task in tasks_to_process:
        await task_queue.put(task)
    print("all tasks queued")

    # Wait for all tasks to be processed
    await task_queue.join()

    # Signal workers to exit by enqueuing None tasks
    for _ in range(num_workers):
        await task_queue.put(None)

    # Wait for all worker tasks to complete
    await asyncio.gather(*worker_tasks)
    await processor

if __name__ == "__main__":
    asyncio.run(main())
