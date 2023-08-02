import os
from datetime import datetime
import queue

import requests
import flask
import yaml
from flask import request
from waitress import serve
from dbftpinterface import DbSFtpInterface

""" Main API server """

UPLOAD_FOLDER = 'httpfiles/'
app = flask.Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Loading settings from config.yaml
config = yaml.unsafe_load(open("config.yaml", 'r').read())

# Initializing MySQL/SFTP connections
middle_to_backend = DbSFtpInterface()
middle_to_backend.db_login(config['auth']['db']['host'], config['auth']['db']['user'],
                           config['auth']['db']['pass'], config['auth']['db']['port'])
middle_to_backend.sftp_login(config['auth']['ftp']['host'], config['auth']['ftp']['user'],
                             config['auth']['ftp']['pass'], config['auth']['ftp']['port'], keepalive=config['auth']['ftp']['keepalive'])

# Jobs are stored in a queue to prevent threads accessing CUDA concurrently
local_job_queue = queue.Queue()


@app.route('/', methods=['POST'])
def clip_from_user():
    """
    Endpoint loads wav and timestamp params from user and pushes to a mixer which handles task assignment.
    Returns 200 if values ar formally correct and the result is available via firebase.

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

    # TODO: Implement exception of given values (wav files corrupted or not compatible with application and timestamp
    #       not being a number.

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] >>> Sending to mixer...")
    params = {
        'wav': request.values['wav'],
        'timestamp': request.values['timestamp']
    }

    requests.post(f"http://{config['mixer']['mixer_ip']}:{config['mixer']['mixer_port']}/", data=params)

    return '', 200


@app.route('/', methods=['GET'])
def default():
    """
    Endpoint that informs to use POST instead of GET.

    Return
    ------
    res : tuple
        Returns static HTML page and 200 HTTP code

    """

    page = "<html>" \
           "<body>" \
           "<p>Use POST instead!</p>" \
           "</body>" \
           "</html>"

    return page, 200


@app.route('/changeId', methods=['POST'])
def change_id():
    """
    Endpoint used to change the speaker id value for a single subclip.
    Returns 200 by default.

    API parameters
    ---------
    id : str
        Subclip hash id.
    user : int
        User id to change.
    new_speaker : int
        New user id.

    Returns
    -------
    res : tuple
        HTTP answer ['', http_code]

    """

    # TODO: Implement exception on given values (e.g. new_speaker not a number) - HTTP 400
    # TODO: Implement exception on backend_error (e.g. offline db) - HTTP 503

    middle_to_backend.change_subclip_user(request.values['id'], request.values['user'], request.values['new_speaker'])
    return '', 200


@app.route('/createUser', methods=['POST'])
def create_user():
    """
    Endpoint used to create a new user with the possibility of generating it from an existing subclip (user is created
    and immediately associated with subclip).

    API Parameters
    ---------
    new_speaker_name : str
        Username given to new user.
    id : int
        Subclip id from which the user is created.

    Returns
    -------
    res : tuple
        HTTP answer ['', http_code]

    """

    # TODO: Implement exception on given values (e.g. id not a number) - HTTP 400
    # TODO: Implement exception on backend_error (e.g. offline db) - HTTP 503

    middle_to_backend.create_speaker(request.values['new_speaker_name'], request.values['id'])
    return '', 200


@app.route('/getSpeakerUsername', methods=['POST'])
def get_username():
    """
    Endpoint used to get the speaker's username from a given id.

    API Parameters
    ---------
    user : int
        User id associated with wanted username.
    speaker : int
        Speaker id which correlates the request with the user that sent the request.

    Returns
    -------
    res : tuple
        HTTP answer [username, http_code]

    """

    # TODO: Implement exception on given values (e.g. user not a number) - HTTP 400
    # TODO: Implement exception on backend_error (e.g. offline db) - HTTP 503

    print(request.values['user'], request.values['speaker'])
    username = middle_to_backend.get_username_from_speaker(request.values['user'], request.values['speaker'])
    return username, 200


@app.route('/deleteSubclip', methods=['POST'])
def delete_subclip():
    """
    Endpoint used to delete a given subclip.

    API Parameters
    ---------
    id : int
        Subclip id to delete.

    res : tuple
        HTTP answer ['', http_code]

    """

    # TODO: Implement exception on given values (e.g. id not a number) - HTTP 400
    # TODO: Implement exception on backend_error (e.g. offline db) - HTTP 503

    middle_to_backend.delete_subclip(request.values['id'])
    return '', 200


if __name__ == "__main__":

    # Loads config for HTTP server startup settings
    config = yaml.unsafe_load(open("config.yaml", 'r').read())

    # Running app via flask on debug while using waitress for production
    if config['httpserver']['debug']:
        print(
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]}] "
            f"Running on server debug")
        app.run(debug=False, host=config['httpserver']['ip'], port=config['httpserver']['port'], use_reloader=False)
    else:
        serve(app, host=config['httpserver']['ip'], port=config['httpserver']['port'],
              threads=config['httpserver']['threads'])
