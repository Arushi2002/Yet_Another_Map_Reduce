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
l = []
for i in range(3):
    x = find_free_port()
    l.append(x)
    p = Popen("python worker.py {}".format(x))


#Creating a Web App
app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False

@app.route("/read",methods=['GET'])
def read():
    response={'network':l}
    return jsonify(response),200

@app.route("/write",methods=['GET'])
def write():
    response={'network':l}
    return jsonify(response),200




app.run(host='0.0.0.0',port=5000)

    