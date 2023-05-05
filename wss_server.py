import asyncio
import hashlib

from mainservice import MainService

import websockets


async def session(websocket):
    print(">>> Connected...")
    clip_bytes = bytes(await websocket.recv())

    clip_hash = hashlib.sha256(clip_bytes).hexdigest()
    with open(f'{clip_hash}.wav', 'wb') as f:
        f.write(clip_bytes)
    print(f">>> Passed to threaded")
    await dedicated_thread_connection(websocket, clip_hash)


async def dedicated_thread_connection(websocket, clip_hash):
    mainapi = MainService()
    result = mainapi.main_job(clip_hash)
    print(result)
    await websocket.send(result)


async def main():
    async with websockets.serve(session, "localhost", 8765, max_size=None):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
