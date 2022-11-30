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
            ans2=ans.split("\r")
            for val in ans2:
                d.write(val)
            #d.write("\n")
        f.close()
    d.close()

@app.route("/mapper",methods=['POST'])
def mapper():
    json=request.get_json()
    input_file = json["input_file"]
    mapper_file=json["mapper"]
    map(input_file, mapper_file)
    response={'message':f'Data successfully mapped into {input_file[:-4]}_map.txt'}
    return jsonify(response),201

def hash(s):
    return len(s)

def shuffle_write(input_file,reducers):
    shuffle_file = input_file[:-4] + "_map" + ".txt"
    print(shuffle_file)
    d = open(shuffle_file, "r")
    lines = d.readlines()
    d.close()
    for i in lines:
        key = i.split()[0]
        print(key)
        hashval = abs(hash(key)%reducers)
        print(hashval)
        output_file  = input_file[:10] + str(hashval)+input_file[11:-4]+"_shuffle" + ".txt"
        print(output_file)
        f = open(output_file, "a")
        f.write(i)
        f.close()
    return


@app.route("/shuffle",methods = ['POST'])
def get_partition():
    json=request.get_json()
    print(json)
    input = json["input_file"]
    num_of_reducers = json["reducers"]
    shuffle_write(input,num_of_reducers)
    response={'message':f'Data successfully shuffled'}
    return jsonify(response),201


# @app.route("/shuffle",methods=['POST'])
# def shuffle():
#     json=request.get_json()
#     intermediate_mapper_file=json["intermediate_mapper_file"]
#     #open intermediate mapper file
#     f=open(intermediate_mapper_file,'r')
#     while(True):
#         line=f.readline()
#         if(line!="\n"):
#             key,val=line.strip().split(,)
#             partition_num=get_partition()
#         else:
#             continue
    
    

app.run(host='0.0.0.0',port = randport)