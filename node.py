import hashlib
import os
import queue
import threading
from datetime import datetime

import flask
import numpy as np
import requests
import yaml
from flask import request

from scipy.io.wavfile import write
from waitress import serve

from dbftpinterface import DbFtpInterface
from mainservice import MainService
from stage1_voic_diar import VoiceDiarization
from stage2_voic_iden import VoiceIdentification

UPLOAD_FOLDER = 'httpfiles/'
app = flask.Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

config = yaml.unsafe_load(open("config.yaml", 'r').read())
if config['httpserver']['cuda_debug']:
    print(
        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] "
        f"Running on cuda debug")
    os.environ['CUDA_LAUNCH_BLOCKING'] = '1'
    os.environ['TORCH_USE_CUDA_DSA'] = '1'
middle_to_backend = DbFtpInterface()
middle_to_backend.db_login(config['auth']['db']['host'], config['auth']['db']['user'],
                           config['auth']['db']['pass'], config['auth']['db']['port'])
middle_to_backend.ftp_login(config['auth']['ftp']['host'], config['auth']['ftp']['user'],
                            config['auth']['ftp']['pass'], config['auth']['ftp']['port'], keepalive=True)
identificator = VoiceIdentification(middle_to_backend, 0.25, config['identification']['device'],
                                    config['identification']['identification_workers'],
                                    config['identification']['levels'])
translator = VoiceDiarization(config['diarization']['model'], config['diarization']['device'],
                              config['diarization']['dualgpu']
                              if config['diarization']['device'] == 'cuda' else False)

local_job_queue = queue.Queue()

@app.route('/job', methods=['POST'])
def addspeaker():
    # Node receives job from mixer and start processing
    # Start processing
    # When ends, remove from unbusy
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] >>> Connected...")
    tmp_file_name = str(int(datetime.utcnow().timestamp()))
    with open(f'tmp{tmp_file_name}.wav', 'wb') as f:
        wav_float_32 = request.values['wav'].strip("[]").split(',')
        write(f, 14000, np.array(wav_float_32, dtype=np.float32))
    size = os.stat(f'tmp{tmp_file_name}.wav')
    timestamp_at_start = request.values['timestamp'].split('/')[-1].split('.')[0]
    with open(f'{"tmp" + tmp_file_name + ".wav"}', 'rb') as f:
        file_to_hash_binary = f.read()
        clip_hash = hashlib.sha256(file_to_hash_binary).hexdigest()
        f.close()
        with open(f'{clip_hash}.wav', 'wb') as g:
            g.write(file_to_hash_binary)
            g.close()
    print(
        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] >>> Passed to threaded; File size: {round(size.st_size / 1024, 1)}kB")

    local_job_queue.put([clip_hash, timestamp_at_start])


    return '', 200

def dedicated_thread():
    print(
        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] "
        f"Main thread started")
    while True:
        print(
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] "
            f"Job started!")
        job = local_job_queue.get()
        clip_hash = job[0]
        timestamp_at_start = job[1]
        mainapi = MainService(translator, identificator, middle_to_backend)
        result, time_took, clip_length_seconds = mainapi.main_job(1, clip_hash, timestamp_at_start)
        print(result)
        print(
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] "
            f"Job took {time_took}s; "
            f"Speed factor {time_took / clip_length_seconds} (lower is better)")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] "
              f"Queue length is {local_job_queue.qsize()}")

        requests.get(f"http://{config['mixer']['mixer_ip']}:{config['mixer']['mixer_port']}/unbusy")
        print("")



if __name__ == "__main__":
    config = yaml.unsafe_load(open("config.yaml", 'r').read())
    if config['node']['httpserver']['debug']:
        print(
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] "
            f"Running on server debug")
        app.run(debug=False, host=config['node']['node_ip'], port=config['node']['node_port'], use_reloader=False)
    else:
        serve(app, host=config['node']['node_ip'], port=config['node']['node_port'],
              threads=config['node']['httpserver']['threads'])
    threading.Thread(target=dedicated_thread).start()