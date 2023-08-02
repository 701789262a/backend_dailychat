import os
from datetime import datetime
import hashlib
import threading
import queue

import numpy as np
import requests
from scipy.io.wavfile import write
import flask
import yaml
from flask import request
from waitress import serve
from dbftpinterface import DbSFtpInterface
from mainservice import MainService
from stage1_voic_diar import VoiceDiarization
from stage2_voic_iden import VoiceIdentification

"""Node to be run on multiple host. Each node will get a job from mixer."""

# Loading Flask app settings
UPLOAD_FOLDER = 'httpfiles/'
app = flask.Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Loading config .yaml file
config = yaml.unsafe_load(open("config.yaml", 'r').read())

# Loading settings for when node is run in debug mode
if config['httpserver']['cuda_debug']:
    print(
        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] "
        f"Running on cuda debug")
    os.environ['CUDA_LAUNCH_BLOCKING'] = '1'
    os.environ['TORCH_USE_CUDA_DSA'] = '1'

# Initializing MySQL/SFTP connections
middle_to_backend = DbSFtpInterface()
middle_to_backend.db_login(config['auth']['db']['host'], config['auth']['db']['user'],
                           config['auth']['db']['pass'], config['auth']['db']['port'])
middle_to_backend.sftp_login(config['auth']['ftp']['host'], config['auth']['ftp']['user'],
                             config['auth']['ftp']['pass'], config['auth']['ftp']['port'],
                             keepalive=config['auth']['ftp']['keepalive'])

# Initializing both sides, identificator and translator
identificator = VoiceIdentification(middle_to_backend, 0.25, config['identification']['device'],
                                    config['identification']['identification_workers'],
                                    config['identification']['levels'])
translator = VoiceDiarization(config['diarization']['model'], config['diarization']['device'],
                              config['diarization']['dualgpu']
                              if config['diarization']['device'] == 'cuda' else False)

# Jobs are stored in a queue to prevent threads accessing CUDA concurrently
local_job_queue = queue.Queue()


@app.route('/job', methods=['POST'])
def clip_from_user():
    """
    Local endpoint loads wav and timestamp params from mixer and loads them into a queue; jobs are taken from LIFO
    queue by dedicated_thread.
    Returns 200 by default, format checks are done by mixer and API server.

    API parameters
    ---------
    wav : binary
        Binary values for .wav clip.
    timestamp : int
        Timestamp value when recording is started.

    Returns
    -------
    res : tuple
        HTTP answer ['', http_code]

    """

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] >>> Connected...")

    # File name used temporary to save clip
    tmp_file_name = str(int(datetime.utcnow().timestamp()))

    # Formatted .wav file is saved with
    with open(f'tmp{tmp_file_name}.wav', 'wb') as f:
        wav_float_32 = request.values['wav'].strip("[]").split(',')
        write(f, 14000, np.array(wav_float_32, dtype=np.float32))

    size = os.stat(f'tmp{tmp_file_name}.wav')
    timestamp_at_start = request.values['timestamp'].split('/')[-1].split('.')[0]

    # Wav hash is calculated. It will be used to refer to clip. Same file is saved with hash as name
    with open(f'{"tmp" + tmp_file_name + ".wav"}', 'rb') as f:
        file_to_hash_binary = f.read()
        clip_hash = hashlib.sha256(file_to_hash_binary).hexdigest()
        f.close()
        with open(f'{clip_hash}.wav', 'wb') as g:
            g.write(file_to_hash_binary)
            g.close()

    # Pushing job referral to queue (hash and timestamp)
    print(
        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] >>> Passed to threaded; File size: {round(size.st_size / 1024, 1)}kB")
    local_job_queue.put([clip_hash, timestamp_at_start])

    return '', 200


def dedicated_thread():
    """Subroutine dedicated to manage every job by just reading from LIFO queue and printing end result
    """

    # Starting log
    print(
        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] "
        f"Main thread started")

    while True:
        print(
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] "
            f"Job started!")

        # Job is taken from queue, split into hash and timestamp. MainService object is reinitialized.
        job = local_job_queue.get()
        clip_hash = job[0]
        timestamp_at_start = job[1]
        mainapi = MainService(translator, identificator, middle_to_backend)

        # Actual task is started
        result, time_took, clip_length_seconds = mainapi.main_job(1, clip_hash, timestamp_at_start)

        # When job is done, statistics are printed and node is unbusied
        print(result)
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] "
              f"Sending unbusy signal")
        requests.get(f"http://{config['node_manager']['ip']}:{config['node_manager']['port']}/unbusy")
        print(
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] "
            f"Job took {time_took}s; "
            f"Speed factor {time_took / clip_length_seconds} (lower is better)")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] "
              f"Queue length is {local_job_queue.qsize()}")
        print("")


# Dedicated thread is started at the beginning
process_thread = threading.Thread(target=dedicated_thread)
process_thread.start()

if __name__ == "__main__":

    # Flask app settings are taken from config.yaml
    config = yaml.unsafe_load(open("config.yaml", 'r').read())
    requests.get(f"http://{config['node_manager']['ip']}:{config['node_manager']['port']}/unbusy")

    # Running app via flask on debug while using waitress for production
    if config['httpserver']['debug']:
        print(
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] "
            f"Running on server debug")
        app.run(debug=False, host=config['node']['node_ip'], port=config['node']['node_port'], use_reloader=False)
    else:
        serve(app, host=config['node']['node_ip'], port=config['node']['node_port'],
              threads=config['httpserver']['threads'])
