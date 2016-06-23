# Plantalytics
#     Copyright (c) 2016 Sapphire Becker, Katy Brimm, Scott Ewing, 
#       Matt Fraser, Kelly Ledford, Michael Limb, Steven Ngo, Eric Turley.
#     This project is licensed under the MIT License.
#     Please see the file LICENSE in this distribution for license terms.
# Contact: plantalytics.capstone@gmail.com

import random
import sys
import time
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Define global string variables
USERNAME = ***
PASSWORD = ***
IP_ADDRESS = ***
KEYSPACE = ***

def get_conditions(vineId, hubId, nodeId):
    # Temp range: 15-85 degrees F
    temperature = random.randint(15, 85)
    # Humidity range: 25-95%
    humidity = random.randint(25, 95)
    # Wetness range: 0-7000 k-Ohms
    leafwetness = random.randint(0, 7000) 
    # Datasent timestamp
    datasent = int(time.time()*1000)
    conditions = (vineId, hubId, nodeId, datasent, temperature,
                  humidity, leafwetness)
    return conditions
    
def generate_dataset(itemCount):
    dataset = []
    for n in range(0, itemCount):
        # For the purposes of demonstration, each vineyard contains 18 nodes.
        v = int(n/18)
        # For the purposes of demonstration, each hub supports 3 nodes.
        h = int(n/3)
        dataset.append(get_conditions(v, h, n))
    return dataset

def create_batch(dataset):
    prologue = ('INSERT INTO ' + KEYSPACE + '.environmental_data ( vineid, '
                'hubid, nodeid, datasent, temperature, humidity, leafwetness, '
                'batchsent ) VALUES ( ')
    epilogue = str(int(time.time()*1000)) + ' );\n'
    batch = 'BEGIN BATCH\n'
    # For each set of conditions in the data set, create a query
    for conditions in dataset:
        query = prologue
        i = 0
        # Add each value in the conditions to the query
        for value in conditions:
            query += str(value) + ', '
        # Add epilogue to insert statement
        query += epilogue
        batch += query
    batch += 'APPLY BATCH;'
    print batch
    return batch
    
def push_batch(batch):
    try:
        auth = PlainTextAuthProvider(username = USERNAME, password = PASSWORD)
        cluster = Cluster(contact_points = [IP_ADDRESS], auth_provider = auth)
        session = cluster.connect(KEYSPACE)
        session.execute(batch)
    except Exception as oops:
        print ('EXCEPTION:\n   ' + str(oops))
    finally:
        session.shutdown()

def bedtime():
    increment = 15
    # Sleep for 5 minutes in 15 second increments
    for t in range(0, 20):
        time.sleep(increment)
    
def main():    
    # Parse the first supplied argument as a positive integer. If this 
    #   succeeds, launch the loop with the speficied input.
    try:
        input = sys.argv[1]
        nodes = int(input)
        assert nodes > 0
    # Otherwise, run with a 9 node sample
    except:
        nodes = 9    
    # Loop until user interrupt
    i = 1
    while 1:
        print ('\n   Dataset no: ' + str(i))
        i += 1
        # Generates CQL batch insertion string for each node
        batch = create_batch(generate_dataset(nodes))
        # Pushes batch to server, executes statement
        push_batch(batch)
        bedtime()
    
if __name__ == "__main__":
    main()
