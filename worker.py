from flask import Flask, jsonify, request
import sys
from subprocess import Popen, PIPE

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
    partition_no = json["partition_no"]
    partition_file_name=f'partition_{partition_no}_{filename}.txt'
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

def map(input_file, mapper_file):
    output_file = input_file[:-4] + "_map" + ".txt"
    print(output_file)
    d = open(output_file, "a")
    with open(input_file, 'r') as f:
        for line in f:
            p = Popen(["python", mapper_file], stdin=PIPE, stdout=PIPE)
            output, err = p.communicate(line.encode('utf-8'))
            ans = output.decode('utf-8')
            print(ans)
            d.write(ans)
        f.close()
    d.close()

@app.route("/mapper",methods=['POST'])
def mapper():
    json=request.get_json()
    input_file = json["input_file"]
    mapper_file=json["mapper"]
    map(input_file, mapper_file)
    response={'message':f'Data successfully mapped into partition_{input_file[:-4]}_node_{randport}_map.txt'}
    return jsonify(response),201

app.run(host='0.0.0.0',port = randport)