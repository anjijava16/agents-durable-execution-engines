import asyncio
import os

from temporalio.client import Client
from temporalio.contrib.deepagents import DeepAgentsPlugin
from temporalio.worker import Worker

from temporal_agent import HelloWorldAgent


async def main() -> None:
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        plugins=[DeepAgentsPlugin()],
    )

    worker = Worker(
        client,
        task_queue="deepagents-hello-world",
        workflows=[HelloWorldAgent],
    )
    print("Worker started. Ctrl+C to exit.")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())