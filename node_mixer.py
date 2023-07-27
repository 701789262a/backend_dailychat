import datetime
import hashlib
import json
import os
import queue
import threading

from scipy.io.wavfile import write
import flask
import numpy as np
import requests
import yaml
from flask import request

node_busy = []
app = flask.Flask(__name__)
config = yaml.unsafe_load(open("config_mixer.yaml", 'r').read())
local_job_queue = queue.Queue()

# Work given
@app.route('/', methods=['POST'])
def job_from_api():
    node_status = json.loads(requests.get(f"http://{config['node_manager_ip']}:{config['node_manager_port']}/").text)
    for node in node_status:
        if node not in node_busy and int(datetime.datetime.now().timestamp()) - int(
                node_status[node]['last_seen']) <= 20:
            params = {
                'wav': request.values['wav'],
                'timestamp': request.values['timestamp']
            }
            node_busy.append(node)
            # Send to node
            print(f"Sending request to node {node} port {config['node_port']}")
            requests.post(f"http://{node}:{config['node_port']}/job", data=params)


@app.route('/unbusy', methods=['GET'])
def unbusy():
    node_busy.remove(request.remote_addr)
    return 200


if __name__ == "__main__":
    app.run(debug=False, host=config['mixer_ip'], port=config['mixer_port'], use_reloader=False)