import asyncio
from app.bots.executor_v2 import executor_loop

async def main():
    task = asyncio.create_task(executor_loop())
    await asyncio.sleep(5)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

asyncio.run(main())
