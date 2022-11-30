from flask import Flask, jsonify, request
import requests
import json
import os


#Taking input from user 
while(True):
    oper = int(input("Enter the operation number that needs to be performed:\n 1. WRITE \n 2. READ \n 3. MAP REDUCE\n 4. EXIT\n "))
    if oper==1:
        filename=input("Enter the name of the file to be written: ")
        response=requests.get(f'http://127.0.0.1:5000/write')
        if response.status_code==200:
            network=response.json()['network']
            num_partitions=len(network)
            f=open(filename,'r') 
            stats = os.stat(filename)
            file_size = stats.st_size#byte size representation
            if(file_size>=num_partitions):
                partition_size = file_size//num_partitions
                # remaining_bytes = file_size-partition_size*num_partitions
                # size = 0
                for i in range (len(network)):  
                    data = ''
                    if i!=len(network)-1:
                        while len(data.encode('utf-8'))<partition_size:
                            data+=f.readline()
                    else:
                        data = ''
                        flag = 1
                        while len(data.encode('utf-8'))<partition_size and flag !=0:
                            line = f.readline()
                            if line == '':
                                flag = 0
                            else:
                                data+=line 
    
                    # print(data)
                    myobj={"data":data,"filename":filename,"node":network[i], 'partition_no':i}
                    url=f'http://127.0.0.1:{network[i]}/write'
                    x = requests.post(url, json = myobj)
                    if x.status_code==201:
                        msg=x.json()
                        print(msg['message'])

            elif(file_size<num_partitions):
                for node in network:
                    if(node==network[0]):
                        data = f.read(file_size)
                    else:
                        #Some partitions be empty data
                        data = ""
                    myobj={"data":data,"filename":filename,"node":node, 'partition_no':i}
                    url=f'http://127.0.0.1:{node}/write'
                    x = requests.post(url, json = myobj)
                    if x.status_code==201:
                        msg=x.json()
                        print(msg['message'])
            f.close()

            
                
    elif oper==2:
        filename=input("Enter the name of the file to be read: ")
        response=requests.get(f'http://127.0.0.1:5000/read')
        if response.status_code==200:
            network=response.json()['network']
            #print(network)
        for i in range(len(network)):
            partition_file_name = f'partition_{i}_{filename[:-4]}.txt'
            myobj={'partition_file_name':partition_file_name}
            #print(partition_file_name)
            response = requests.post(f'http://127.0.0.1:{network[i]}/read',json=myobj)
            if response.status_code==201:
                data=response.json()
                print(data['data'],end="")
        print()

    elif oper==3:
        input_file=input("Enter the name of the input file: ")
        mapper=input("Enter the name of the mapper file: ")
        reducer=input("Enter the name of the reducer file: ")

        #response=requests.get(f'http://127.0.0.1:5000/map_ack')
        args_obj = {"input_file":input_file, "mapper":mapper, "reducer":reducer}
        response = requests.post('http://127.0.0.1:5000/map_reduce', json=args_obj)

        if response.status_code==201:
            print("Map Stage completed")
        
            # network=response.json()['network']
            
            # for node in network:
            #     myobj={"input_file":f'partition_{input_file[:-4]}_node_{node}.txt',"mapper":mapper}
            #     url=f'http://127.0.0.1:{node}/mapper'
            #     x = requests.post(url, json = myobj)
            #     if x.status_code==201:
            #         msg=x.json()
            #         print(msg['message'])

           
        else:
            print("Map Failed")
    elif oper==4:
        exit()
    else:
        print("Invalid operation!")