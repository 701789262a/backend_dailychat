import datetime
import datetime
import hashlib
import threading

import flask
import yaml
from flask import request

from mainservice import MainService

UPLOAD_FOLDER = 'httpfiles/'
app = flask.Flask(__name__)
app.config["DEBUG"] = True
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


@app.route('/', methods=['POST'])
def addspeaker():
    print(">>> Connected...")
    tmp_file_name = str(int(datetime.datetime.utcnow().timestamp()))
    with open(f'tmp{tmp_file_name}.wav', 'wb') as f:
        request.files['file'].save(f)

    timestamp_at_start = request.values['timestamp'].split('/')[-1].split('.')[0]
    with open(f'{"tmp" + tmp_file_name + ".wav"}', 'rb') as f:
        file_to_hash_binary = f.read()
        clip_hash = hashlib.sha256(file_to_hash_binary).hexdigest()
        f.close()
        with open(f'{clip_hash}.wav', 'wb') as g:
            g.write(file_to_hash_binary)
            g.close()
    print(f">>> Passed to threaded")

    threading.Thread(target=dedicated_thread_connection, args=(clip_hash, timestamp_at_start,)).start()
    return '', 200


def dedicated_thread_connection(clip_hash, timestamp_at_start):
    mainapi = MainService()
    result, time_took, clip_length_seconds = mainapi.main_job(1, clip_hash, timestamp_at_start)
    print(result)
    print(f"Job took {time_took}s; Speed factor {time_took/clip_length_seconds} (lower is better)")


if __name__ == "__main__":
    config = yaml.unsafe_load(open("config.yaml", 'r').read())
    app.run(debug=True, host=config['httpserver']['ip'], port=config['httpserver']['port'])
