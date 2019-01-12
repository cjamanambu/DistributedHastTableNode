# DistributedHastTableNode

The file can be run with 'python dht.py'.
Activity and Error logs are written to stdout regularly based on the state of the ring.
The node will stabilize it's immediate surrounding on the ring in 30 second intervals.
To generate a query, simply press any key on the keyboard. If the query is invalid (Not an Integer, or an Integer less than or equal to the node's ID) a valid query (an Integer greater than the node's ID will be generated) and a find protocol will be sent to the node's current successor.
An owner protocol, when received, will log the details in stdout.
Pressing the enter key alone will shutdown the node and exit the ring.