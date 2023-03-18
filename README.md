# Map Reduce Replicated

Implemented the core concepts of Hadoop's Map Reduce Framework.

Can handle big data computation with enough modularity for the user to input any mapper/reducer appropriately. 





## Overview of the project

We have setup a multinode environment consisting of a master node and multiple worker nodes.
A client program communicates with the nodes based on the types of operations requested by the user.
The types of operations handled by this project are:
* WRITE: Given an input file, split it into multiple partitions and store it across multiple worker nodes.
* READ: Given a file name, read the different partitions from different workers and display it to the user.
* MAP-REDUCE - Given an input file, a mapper file and a reducer file, execute a MapReduce Job on the cluster.




## Requirements
Install python 3.8

 Packages required -
 1. Flask
 2. requests
 3. json
 4. contextlib
 5. subprocess
 6. os

 

## To run the project

Run the client file and the master_node file on 2 seperate terminals by using the following commands-
```
python client.py
```
```
python master_node.py
```
