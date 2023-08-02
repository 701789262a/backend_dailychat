import datetime
import json
import queue
import flask
import requests
import yaml
from flask import request

# Busy node list
node_busy = []

app = flask.Flask(__name__)
config = yaml.unsafe_load(open("config_mixer.yaml", 'r').read())

# Queue to store job
# TODO: Implement waiting queue if all node are busy
local_job_queue = queue.Queue()


# Work given
@app.route('/', methods=['POST'])
def job_from_api():
    """API endpoint receives job from API server and assign task to a node.
    When assignment is done, node is flagged as busy

    API Parameters
    ---------
    wav : binary
        Binary values for .wav clip.
    timestamp : int
        Timestamp value when recording is started.

    Returns
    -------
    res : tuple
        Empty page with 200 HTTP status code.

    TODO: if node doesn't answer (crashed in the last few seconds [<20]) job is passed to next node

    """

    # Getting status sample from node manager
    node_status = json.loads(requests.get(f"http://{config['node_manager_ip']}:{config['node_manager_port']}/").text)

    # Iterating through all time (from node manager startup) active nodes
    for node in node_status:

        # Checking if node is not currently flagged as busy and if the last polling was done within 20 seconds
        if node not in node_busy and \
                int(datetime.datetime.now().timestamp()) - int(node_status[node]['last_seen']) <= 20:

            # Preparing parameters to send
            params = {
                'wav': request.values['wav'],
                'timestamp': request.values['timestamp']
            }

            # Flagging node as busy
            node_busy.append(node)

            # Sending job to node
            print(f"Sending request to node {node} port {config['node_port']}")
            requests.post(f"http://{node}:{config['node_port']}/job", data=params)

            return '', 200


@app.route('/unbusy', methods=['GET'])
def unbusy():
    """API endpoint to flag a node as free after completing a job.
    If node is not flagged as busy (every node sends an unbusy signal at startup) it simply ignores.

    This requests should be sent by node_manager, which acts as relay from node

    API Parameters
    ---------
    ip : str
        Node IP to set free

    Returns
    -------
    res : tuple
        Empty page and 200 HTTP status code

    """

    print(request.values['ip'])

    # Removes node from busy list
    try:
        node_busy.remove(request.values['ip'])

    # If a ValueError is raised, the node is already seed free by mixer and the exception is ignored
    except ValueError:
        pass

    return '', 200


if __name__ == "__main__":
    app.run(debug=False, host=config['mixer_ip'], port=config['mixer_port'], use_reloader=False)
