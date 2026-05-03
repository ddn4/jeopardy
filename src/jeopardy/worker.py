import asyncio

from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Worker

from . import TASK_QUEUE
from .activities import (
    judge_answer,
    persist_result,
    select_random_game,
    select_temporal_game,
)
from .workflows import JeopardyGameWorkflow


async def main() -> None:
    client = await Client.connect(
        "localhost:7233",
        data_converter=pydantic_data_converter,
    )
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[JeopardyGameWorkflow],
        activities=[
            judge_answer,
            persist_result,
            select_random_game,
            select_temporal_game,
        ],
    )
    print(f"Worker ready, polling task queue '{TASK_QUEUE}'", flush=True)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
