# environmental-conditioner

A random number generator to simulate the transmission of node data to a Cassandra database.

## Use

Edit file to set global variables for your Cassandra instance username, password, IP address, and target keyspace.

If no arguments are supplied, or a non-integer argument is passed, environmental_conditioner will generate 9 nodes worth of data. A valid, positive integer argument will generate the specified number of nodes worth of data. Eg:

    python environmental_conditioner.py

results in 9 nodes, whereas

    python environmental_conditioner.py 48

results in 48 nodes.

Barring a fatal error, environmental_conditioner will run indefinitely and can be terminated with an abort command (CTRL-C, etc).

## License

Copyright (c) 2016 Sapphire Becker, Katy Brimm, Scott Ewing, Matt Fraser, Kelly Ledford, Michael Limb, Steven Ngo, Eric Turley.

This project is licensed under the MIT License. Please see the file LICENSE in this distribution for license terms.