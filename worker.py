from flask import Flask, jsonify, request
import sys
randport = int(sys.argv[1])

#Creating a Web App
app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False

@app.route("/write",methods=['POST'])
def write():
    json=request.get_json()
    data=json["data"]
    filename=json["filename"][:-4]
    node=json["node"]
    partition_file_name=f'partition_{filename}_node_{node}.txt'
    f = open(partition_file_name, "w")
    f.write(data)
    f.close()
    response={'message':f'Data successfully written to {partition_file_name}','partition_file_name':partition_file_name}    
    return jsonify(response), 201

@app.route("/read",methods=['POST'])
def read():
    json=request.get_json()
    partition_file_name=json["partition_file_name"]
    f = open(partition_file_name, "r")
    data=f.read()
    f.close()
    response={'data':data}
    return jsonify(response),201

app.run(host='0.0.0.0',port = randport)