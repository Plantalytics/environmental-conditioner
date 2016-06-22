# Author: Katy Brimm
# Contact: kbrimm@pdx.edu
# Date: 06/16/16
# Use: Generates 9 sets of random (temperature, humidity, leaf wetness) tuples
#   every five minutes, transmits to Apache Cassandra DB.
# License: This source file is copyright (c) Katy Brimm and licensed under the
#   MIT License. In other words: take it, use it, love it.
# 	Please see the file LICENSE included in this distribution for terms.

import random
import sys
import time
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

def get_conditions(hubId, nodeId):
    # Temp range: 15-85 degrees F
    temperature = random.randint(15, 85)
    # Humidity range: 5-95%
    humidity = random.randint(5, 95)
    # Wetness range: 0-7000 k-Ohms
    leafwetness = random.randint(0, 7000) 
    conditions = (hubId, nodeId, int(time.time()*1000), temperature,
                  humidity, leafwetness)
    return conditions
    
def generate_dataset(itemCount):
    dataset = []
    for n in range(0, itemCount):
        # For the purposes of demonstration, each hub supports three nodes.
        h = int(n/3)
        dataset.append(get_conditions(h, n))
    return dataset

def create_batch(dataset):
    prologue = ('INSERT INTO capstone_plantalytics.environmental_data ( '
                'hubid, nodeid, datasent, temperature, humidity, leafwetness, '
                'batchsent ) VALUES ( ')
    epilogue = ' );\n'
    batch = 'BEGIN BATCH\n'
    # For each set of conditions in the data set, create a query
    for conditions in dataset:
        query = prologue
        i = 0
        # Add each value in the conditions to the query
        for value in conditions:
            query += str(value) + ', '
        # Add a batch sent timestamp
        query += str(int(time.time()*1000))
        query += epilogue
        batch += query
    batch += 'APPLY BATCH;'
    print batch
    return batch
    
def push_batch(batch):
    auth = PlainTextAuthProvider(username=*username*, password=*password*)
    cluster = Cluster(contact_points=[*ip*], auth_provider=auth)
    session = cluster.connect(*keyspace*)
    session.execute(batch)
    session.shutdown()

def bedtime():
    increment = 15
    # Sleep for 5 minutes in 15 second increments
    for t in range(0, 20):
        time.sleep(increment)
    
def main():
    i = 1
    while 1:
        print ('Dataset no: ' + str(i))
        i += 1
        # Generates 9 nodes of data
        push_batch(create_batch(generate_dataset(9)))
        bedtime()
    
if __name__ == "__main__":
    main()
