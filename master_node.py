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
    p = Popen("python worker.py {}".format(rand_port), shell=True)


#Creating a Web App
app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False

#Spawning worker nodes dynamically
# @app.route("/nodes",methods=['GET'])
# def nodes():
#     response={'network':worker_ports}
#     return jsonify(response),200

# @app.route("/number",methods=['POST'])
# def number():
#     json=request.get_json()
#     data=json["workers"]
#     for i in range(data):
#         x = find_free_port()
#         worker_ports.append(x)
#         p = Popen("python worker.py {}".format(x), shell=True)
#     response={'message':f'Created {data} workers successfully'}    
#     return jsonify(response), 201

#read
@app.route("/read",methods=['GET'])
def read():
    response={'network':worker_ports}
    return jsonify(response),200

#write
@app.route("/write",methods=['GET'])
def write():
    response={'network':worker_ports}
    return jsonify(response),200

#map_ack
@app.route("/map_ack",methods=['POST'])
def map_ack():
    #network=response.json()['network']
    json_obj=request.get_json()
    response={'network':worker_ports}
    input_file = json_obj["input_file"]
    mapper = json_obj["mapper"]
    for i,node in enumerate(worker_ports):
        myobj={"input_file":f'partition_{i}_{input_file[:-4]}.txt',"mapper":mapper}
        url=f'http://127.0.0.1:{node}/mapper'
        response_mapper = requests.post(url, json = myobj)
        if response_mapper.status_code==201:
            msg=response_mapper.json()
            print(msg['message'])
        # else:
        #     print(msg['message'])
    
    return jsonify(response),201

app.run(host='0.0.0.0',port=5000) 