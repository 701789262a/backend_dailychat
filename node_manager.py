import os
import threading
import time
import datetime

import requests
import yaml
from flask import request
from netaddr import IPNetwork
import socket
from rich.console import Console
from rich.table import Table

import flask

# Node manager Flask app is started
app = flask.Flask(__name__)

# Final dictionary containing ip value
final = {}


def check_port(ip, port):
    """Thread function to check one single IP on a given port.

    Arguments
    ---------
    ip : str
        IP given as a string "aaa.bbb.ccc.ddd"
    port : int
        Port to check on given ip

    """

    try:
        # Initializing socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        time_start = datetime.datetime.now().timestamp()

        # Connecting to given IP
        result = sock.connect_ex((ip, port))
        time_stop = datetime.datetime.now().timestamp()

        # Connect_ex returns 0 if connection is successful
        if result == 0:
            # IP is added/updated within dictionary
            final[ip] = {
                "last_seen": f"{int(datetime.datetime.now().timestamp())}",
                "latency": f"{round((time_stop - time_start) * 1000, 3)} ms"
            }

        # Socket is closed
        sock.close()

    # In case of error (e.g. timeout) pass
    # TODO: specify except conditions
    except:
        pass


class Manager:
    def __init__(self):

        # Config will be assigned as YAML config file
        self.config = None

        # Console will be assigned as rich console
        self.console = None

        # Table will contain rich table
        self.table = None

    def display_table(self):
        """Thread function that constantly refresh console with updated table.
        """

        while True:

            # Table is updated every 4 seconds
            time.sleep(4)
            try:

                # Every IP in final dict (IPs with valid connection are stored there) is added to table
                for ip in final:
                    if ip in final.keys():
                        # Table is updated
                        self.table.add_row(
                            ip,
                            datetime.datetime.fromtimestamp(int(final[ip]['last_seen'])).strftime('%Y-%m-%d %H:%M:%S'),
                            final[ip]['latency'])

                        # Terminal is cleared and new table is pushed to console
                        if os.name == 'nt':
                            os.system('cls')
                        elif os.name == 'posix':
                            os.system('clear')

                        self.console.print(self.table)
            except:
                pass

    def main_job(self):
        """Routine started when /start API endpoint is invoked.
        Every 2 seconds a socket connection check each address on the subnet of the provided network address is
        initialized.

        """

        # Loading rich console and config from .yaml file
        self.console = Console()
        self.config = yaml.unsafe_load(open("config_node_manager.yaml", 'r').read())

        # Initializing network cidr blocks and port
        port = self.config['maintenance_port_node']
        cidr_blocks = self.config['cidr_blocks']

        # Table display thread is started
        threading.Thread(target=self.display_table).start()

        # Core infinite loop that runs every 2 seconds
        while True:
            time.sleep(2)

            # Table is reinitialized every time (not possible to delete rows manually)
            self.table = Table(title='Online nodes')
            self.table.add_column('IP address')
            self.table.add_column('Last seen')
            self.table.add_column('Ping')

            # Generating address spaces (generating an IPNetwork iterable for every cidr_blocks given)
            address_spaces = [IPNetwork(cidr_block) for cidr_block in cidr_blocks]

            # Iterating through cidr_blocks
            for address_space in address_spaces:

                # Iterating through every IP
                for ip in address_space.iter_hosts():

                        # Thread to scan one single IP is started. Results will be pushed to final dictionary
                        threading.Thread(target=check_port, args=[str(ip), port]).start()


@app.route('/', methods=['GET'])
def get_status():
    """API endpoint to return status to node_mixer.

    Returns
    -------
    res : tuple
        Node status and default HTTP status code 200
    """

    return final, 200


@app.route('/start', methods=['GET'])
def start():
    """API endpoint to start main_job routine on a thread.

    Returns
    -------
    res : tuple
        Empty page and default HTTP status code 200
    """
    m = Manager()
    threading.Thread(m.main_job()).start()
    return '', 200


@app.route('/unbusy', methods=['GET'])
def unbusy():
    """API endpoint to unbusy given ip from mixer local variable.

    API Parameters
    ---------
    remote_addr : str
        Remote IP address is used as an identifier for node

    Returns
    -------
    res : tuple
        Empty page and default HTTP status code 200

    """
    requests.get(f'http://{config["mixer_ip"]}:{config["mixer_port"]}/unbusy', params={'ip': request.remote_addr})
    return '', 200


if __name__ == "__main__":
    # Loading config for running app on chosen IP:port
    config = yaml.unsafe_load(open("config_node_manager.yaml", 'r').read())
    print(f" * Visit {config['httpserver']['ip']}:{config['httpserver']['port']}/start to start node manager")
    app.run(debug=False, host=config['httpserver']['ip'], port=config['httpserver']['port'], use_reloader=False)
