import asyncio
import hashlib

from websockets.sync.client import connect


async def hello():
    with connect("ws://localhost:8765") as websocket:
        with open("gianmarco3.wav", 'rb') as f:
            binary_file = f.read()
            clip_hash=hashlib.sha256(binary_file).hexdigest()
            print(f"clip sent: {clip_hash}")

        websocket.send(binary_file)
        message = websocket.recv()
        print(f"Received: {message}")


asyncio.run(hello())
