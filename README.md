# ChordDFS
Distributed File System implementation using Chord algorithm

## Requirements
- Mininet 2.1.0
- MiniNExT
- Python 2 or 3


## Instructions
1. Run `sudo python start.py --num_nodes` where `num_nodes` is the number of nodes in your topology that you want to start.
2. Start Client protocol on node 2 by running `node# python Chord.py n2 \n2`; ie, `n2 python Client.py n2 \n2`.

Note that currently n1 is the tracker node and n2 is set up to be the client node by default.

## Examples
```
n1 python Chord.py n1 \n1 &					# n1 run server in background
n2 python Client.py n2 \n2					# n2 run client with stdin i/o
n3 python Client.py n3 \n3 script.txt 		# n3 run client with script, no i/o
```