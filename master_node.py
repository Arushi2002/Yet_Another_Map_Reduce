from flask import Flask, jsonify, request
import requests

#Creating a Web App
app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False

@app.route("/read",methods=['GET'])
def read():
    response={'network':[5001,5002,5003]}
    return jsonify(response),200

@app.route("/write",methods=['GET'])
def write():
    response={'network':[5001,5002,5003]}
    return jsonify(response),200




app.run(host='0.0.0.0',port=5000)

    