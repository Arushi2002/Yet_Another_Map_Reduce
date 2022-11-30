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

# mapper function, and its mapping
from subprocess import Popen, PIPE
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
    response={'message':f'Data successfully mapped into {input_file}_map'}
    return jsonify(response),201

# hasher function and its hashing
# from subprocess import Popen, PIPE
def hash(input_file, hash_file, networks):
    with open(input_file, 'r') as f:
        no = len(networks)
        for line in f:
            p = Popen(f"python {hash_file} {no}", stdin=PIPE, stdout=PIPE)
            output, err = p.communicate(line.encode('utf-8'))
            ans = int(output.decode('utf-8'))
            print(ans)
            output_file = f'partition_{input_file[:-4]}_node_{networks[ans]}_hash.txt'
            print(output_file)
            d = open(output_file, "a")
            d.write(line)
            d.close()
        f.close()

@app.route("/hasher",methods=['POST'])
def hasher():
    json=request.get_json()
    input_file = json["input_file"]
    hash_file=json["hash"]
    networks=json["network"]
    hash(input_file, hash_file, networks)
    response={'message':f'Data successfully hashed from {input_file}'}
    return jsonify(response),201

app.run(host='0.0.0.0',port = randport)