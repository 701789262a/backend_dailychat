import asyncio
import datetime
import hashlib
import threading

import flask
from mainservice import MainService

import websockets
import os
from flask import Flask, flash, request, redirect, url_for
from werkzeug.utils import secure_filename
UPLOAD_FOLDER = 'httpfiles/'
app = flask.Flask(__name__)
app.config["DEBUG"] = True
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

@app.route('/',methods=['POST'])
def addspeaker():
    print(">>> Connected...")
    tmp_file_name = str(int(datetime.datetime.utcnow().timestamp()))
    with open(f'tmp{tmp_file_name}.wav','wb')as f:
        request.files['file'].save(f)

    timestamp_at_start=request.values['timestamp'].split('/')[-1].split('.')[0]

    with open(f'{"tmp"+tmp_file_name+".wav"}', 'rb') as f:
        file_to_hash_binary=f.read()
        clip_hash = hashlib.sha256(file_to_hash_binary).hexdigest()
        f.close()
        with open(f'{clip_hash}.wav','wb') as g:
            g.write(file_to_hash_binary)
            g.close()
    print(f">>> Passed to threaded")

    threading.Thread(target=dedicated_thread_connection,args=(clip_hash, timestamp_at_start,)).start()
    return '',200


def dedicated_thread_connection(clip_hash, timestamp_at_start):
    mainapi = MainService()
    result,time_took = mainapi.main_job(1, clip_hash, timestamp_at_start)
    print(result)
    print(f"Job took {time_took}s")


if __name__ == "__main__":
    app.run(debug=True,host='0.0.0.0')
