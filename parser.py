import asyncio as aio
import sys
import warnings as w
w.filterwarnings('ignore')
from requests import Session as ClientSession
from requests import adapters
from core.Parser import Parser


async def main():
    ifBySkuList = False
    parser = Parser(ifBySkuList)
    with ClientSession() as session:
        adapter = adapters.HTTPAdapter(pool_connections=30, pool_maxsize=100)
        session.mount('https://', adapter)
        session.mount('http://', adapter)
        await parser.parse(session, enable_proxies=True, ifBySkuList=ifBySkuList)


try:
    loop = aio.get_running_loop()
except RuntimeError:  # 'RuntimeError: There is no current event loop...'
    loop = None

if loop and loop.is_running():
    print('Async event loop already running. Adding coroutine to the event loop.')
    tsk = loop.create_task(main())
    # ^-- https://docs.python.org/3/library/asyncio-task.html#task-object
    # Optionally, a callback function can be executed when the coroutine completes
    tsk.add_done_callback(
        lambda t: print(f'Task done with result={t.result()}  << return val of main()'))
else:
    print('Starting new event loop')
    result = aio.run(main())
