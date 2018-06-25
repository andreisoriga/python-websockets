import asyncio
import json
import websockets
import logging

STATE = {'value': 0}

CLIENTS = set()

logger = logging.getLogger('websockets')
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


def state_event():
    return json.dumps({'type': 'state', **STATE})


def users_event():
    return json.dumps({'type': 'users', 'count': len(CLIENTS)})


async def notify_state():
    # asyncio.wait doesn't accept an empty list
    if CLIENTS:
        message = state_event()
        await asyncio.wait([client.send(message) for client in CLIENTS])


async def notify_users():
    # asyncio.wait doesn't accept an empty list
    if CLIENTS:
        message = users_event()
        await asyncio.wait([client.send(message) for client in CLIENTS])


async def register(websocket):
    logger.info('New Client connected.')
    CLIENTS.add(websocket)
    await notify_users()


async def unregister(websocket):
    logger.info('Client disconnected.')
    CLIENTS.remove(websocket)
    await notify_users()


async def counter(message):
    data = json.loads(message)
    if data['action'] == 'minus':
        STATE['value'] -= 1
        await notify_state()
    elif data['action'] == 'plus':
        STATE['value'] += 1
        await notify_state()


async def consumer_handler(websocket, path, queue):
    async for message in websocket:
        await counter(message)
        await queue.put(message)


async def producer_handler(websocket, path, queue):
    while True:
        if not queue.empty():
            message = await queue.get()
            message = json.dumps({'type': 'msg', 'msg': message})

            await asyncio.wait([client.send(message) for client in CLIENTS])

        await asyncio.sleep(0.1)


async def handler(websocket, path):

    consumerq = asyncio.Queue()
    producerq = asyncio.Queue()

    # register(websocket) sends user_event() to websocket
    await register(websocket)

    try:

        await websocket.send(state_event())

        consumer_task = asyncio.ensure_future(consumer_handler(websocket, path, consumerq))

        producer_task = asyncio.ensure_future(producer_handler(websocket, path, producerq))

        process_task = asyncio.ensure_future(worker_handler(consumerq, producerq))

        done, pending = await asyncio.wait(
            [consumer_task, producer_task, process_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()

    finally:
        await unregister(websocket)


async def worker_handler(consumerq, producerq):

    while True:
        if not consumerq.empty():
            item = await consumerq.get()

            # simulate i/o operation using sleep
            # await asyncio.sleep(random.random())

            await producerq.put(
                json.dumps({'type': 'msg', 'msg': f'got {item} from consumerq (qsize {consumerq.qsize()})'})
            )

            consumerq.task_done()

        # wait until the consumer has processed all items
        await consumerq.join()

        await asyncio.sleep(0.1)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(websockets.serve(handler, 'localhost', 6789))
    asyncio.get_event_loop().run_forever()
