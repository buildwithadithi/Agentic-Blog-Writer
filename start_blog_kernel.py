import asyncio
from ipykernel.kernelapp import IPKernelApp

asyncio.set_event_loop_policy(
    asyncio.WindowsSelectorEventLoopPolicy()
)

IPKernelApp.launch_instance()