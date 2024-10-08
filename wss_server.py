import os
from datetime import datetime
import hashlib
import threading
import queue

import numpy as np
from scipy.io.wavfile import write
import flask
import yaml
from flask import request
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

# Jobs are stored in a queue to prevent threads accessing CUDA concurrently
local_job_queue = queue.Queue()


@app.route('/', methods=['POST'])
def addspeaker():
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


@app.route('/', methods=['GET'])
def default():
    page = "<html>" \
           "<body>" \
           "<p>Use POST instead!</p>" \
           "</body>" \
           "</html>"
    return page, 200


@app.route('/changeId', methods=['POST'])
def change_id():
    middle_to_backend.change_subclip_user(request.values['id'], request.values['user'], request.values['new_speaker'])
    return '', 200


@app.route('/createUser', methods=['POST'])
def create_user():
    middle_to_backend.create_speaker(request.values['new_speaker_name'], request.values['id'])
    return '', 200


@app.route('/getSpeakerUsername', methods=['POST'])
def get_username():
    print(request.values['user'], request.values['speaker'])
    username = middle_to_backend.get_username_from_speaker(request.values['user'], request.values['speaker'])
    return username, 200


@app.route('/deleteSubclip', methods=['POST'])
def delete_subclip():
    middle_to_backend.delete_subclip(request.values['id'])
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
        print("")


threading.Thread(target=dedicated_thread).start()
if __name__ == "__main__":
    config = yaml.unsafe_load(open("config.yaml", 'r').read())

    if config['httpserver']['debug']:
        print(
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] "
            f"Running on server debug")
        app.run(debug=False, host=config['httpserver']['ip'], port=config['httpserver']['port'], use_reloader=False)
    else:
        serve(app, host=config['httpserver']['ip'], port=config['httpserver']['port'],
              threads=config['httpserver']['threads'])
