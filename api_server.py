import os
from datetime import datetime
import queue

import requests
import flask
import yaml
from flask import request
from waitress import serve
from dbftpinterface import DbSFtpInterface

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
middle_to_backend = DbSFtpInterface()
middle_to_backend.db_login(config['auth']['db']['host'], config['auth']['db']['user'],
                           config['auth']['db']['pass'], config['auth']['db']['port'])
middle_to_backend.sftp_login(config['auth']['ftp']['host'], config['auth']['ftp']['user'],
                            config['auth']['ftp']['pass'], config['auth']['ftp']['port'], keepalive=True)

# Jobs are stored in a queue to prevent threads accessing CUDA concurrently
local_job_queue = queue.Queue()


@app.route('/', methods=['POST'])
def addspeaker():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] >>> Sending to mixer...")
    params = {
        'wav':request.values['wav'],
        'timestamp':request.values['timestamp']
    }

    requests.post(f"http://{config['mixer']['mixer_ip']}:{config['mixer']['mixer_port']}/",data=params)



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
