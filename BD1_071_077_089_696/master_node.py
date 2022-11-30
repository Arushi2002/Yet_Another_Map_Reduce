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


#Creating a Web App
app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False

#Spawning worker nodes dynamically
@app.route("/nodes",methods=['GET'])
def nodes():
    response={'network':l}
    return jsonify(response),200

l = []
@app.route("/number",methods=['POST'])
def number():
    json=request.get_json()
    data=json["workers"]
    for i in range(data):
        x = find_free_port()
        l.append(x)
        p = Popen("python worker.py {}".format(x))
    response={'message':f'Created {data} workers successfully'}    
    return jsonify(response), 201

#read
@app.route("/read",methods=['GET'])
def read():
    response={'network':l}
    return jsonify(response),200

#write
@app.route("/write",methods=['GET'])
def write():
    response={'network':l}
    return jsonify(response),200

#map_ack
@app.route("/map_ack",methods=['GET'])
def map_ack():
    response={'network':l}
    return jsonify(response),200

app.run(host='0.0.0.0',port=5000) 