import os
import threading
import time
import datetime

import requests
import yaml
from flask import request
from netaddr import IPNetwork
import socket
from rich.live import Live
from rich.console import Console
from rich.table import Table

import flask
app = flask.Flask(__name__)
final = {}
table_status={}
class Manager():
    def __init__(self):
        self.config = None
        self.console = None
        self.table = None

    def check_port(self,ip, port):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # TCP
            #sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP
            socket.setdefaulttimeout(2.0) # seconds (float)
            start = datetime.datetime.now().timestamp()
            result = sock.connect_ex((ip,port))
            stop = datetime.datetime.now().timestamp()
            if result == 0:
                # print ("Port is open")
                final[ip] = {"last_seen":f"{int(datetime.datetime.now().timestamp())}","latency":f"{round((stop-start)*1000,3)} ms"}
            sock.close()
        except:
            pass

    def display_table(self):
        while True:
            time.sleep(4)
            try:
                for ip in final:
                    if ip in final.keys():
                        table_status[ip]='OPEN'
                        self.table.add_row(ip,datetime.datetime.fromtimestamp(int(final[ip]['last_seen'])).strftime('%Y-%m-%d %H:%M:%S'),final[ip]['latency'])
                        os.system('cls')
                        self.console.print(self.table)
            except:
                pass


    def main_job(self):
        self.console = Console()
        self.config = yaml.unsafe_load(open("config_node_manager.yaml", 'r').read())

        network_address = self.config['network_address']
        subnet_mask = self.config['subnet_mask']
        port = self.config['maintenance_port_node']
        subnet_mask_bin = ""
        for tip in subnet_mask.split('.'):
            subnet_mask_bin += format(int(tip),'08b')
        cidr = str(subnet_mask_bin.count('1'))
        threading.Thread(target=self.display_table).start()
        while True:
            time.sleep(2)
            self.table = Table(title='Online nodes')
            self.table.add_column('IP address')
            self.table.add_column('Last seen')
            self.table.add_column('Ping')
            for ip in IPNetwork('/'.join([network_address,cidr])):
                if not str(ip) == str(IPNetwork('/'.join([network_address,cidr])).broadcast) and not str(ip) == str(IPNetwork('/'.join([network_address,cidr])).network):
                    threading.Thread(target=self.check_port, args=[str(ip), port]).start()



@app.route('/', methods=['GET'])
def get_status():
    return final, 200

@app.route('/start', methods=['GET'])
def start():
    m = Manager()
    threading.Thread(m.main_job()).start()
    return '',200

@app.route('/unbusy', methods=['GET'])
def unbusy():
    requests.get(f'http://{config["mixer_ip"]}:{config["mixer_port"]}/',params=request.remote_addr)
    return '',200

if __name__ == "__main__":
    config = yaml.unsafe_load(open("config_node_manager.yaml", 'r').read())

    app.run(debug=False, host=config['httpserver']['ip'], port=config['httpserver']['port'], use_reloader=False)





