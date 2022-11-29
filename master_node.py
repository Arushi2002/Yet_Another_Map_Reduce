from flask import Flask, jsonify, request
import requests
import socket
from contextlib import closing
from subprocess import Popen, PIPE

def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]

no_of_worker_nodes = int(input("Enter the number of worker nodes: "))
worker_ports = []
for i in range(no_of_worker_nodes):
    rand_port = find_free_port()
    worker_ports.append(rand_port)
    p = Popen("python3 worker.py {}".format(rand_port), shell=True)


#Creating a Web App
app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False

@app.route("/read",methods=['GET'])
def read():
    response={'network':worker_ports}
    return jsonify(response),200

@app.route("/write",methods=['GET'])
def write():
    response={'network':worker_ports}
    return jsonify(response),200

@app.route("/map", methods=['GET'])
def map():
    response = {'network':worker_ports}
    return jsonify(response), 200


app.run(host='0.0.0.0',port=5000)

    