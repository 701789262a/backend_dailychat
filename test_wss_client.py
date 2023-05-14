import asyncio
import hashlib
import requests
from websockets.sync.client import connect


def main():
    files = {'file': open('gianmarco3.wav', 'rb')}
    data = {'timestamp':'10000000'}
    print(requests.post('http://192.168.0.10:5000/',files=files,params=data).status_code)

if __name__ == "__main__":
    main()