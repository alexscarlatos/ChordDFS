"""
Topology of ChordDFS routers
"""

import inspect
import os
import sys
  
from mininext.topo import Topo
from mininext.services.quagga import QuaggaService
from collections import namedtuple

ChordDFSHost = namedtuple("ChordDFSHost", "name ip")
net = None

class ChordDFSTopo(Topo):
	"Creates a topology of ChordDFS routers"

	def __init__(self, num_nodes):
		"""Initialize a ChordDFS topology with num_nodes nodes, configure their IP
		addresses and paths to their private directories"""
		Topo.__init__(self)

		chordDFSHosts = []

		# create nodes
		for node in range(num_nodes):		
			host = self.addHost(name='n{0}'.format(node+1), ip='172.1.1.{0}/24'.format(node+1))
			chordDFSHosts.append(host)
			# create corresponding directories
			if not os.path.exists("nodes/n{0}".format(node+1)):
				os.makedirs("nodes/n{0}".format(node+1))
				os.makedirs("nodes/n{0}/files".format(node+1))
				os.makedirs("nodes/n{0}/files/chord".format(node+1))
				os.makedirs("nodes/n{0}/files/client".format(node+1))
				os.makedirs("nodes/n{0}/logs".format(node+1))
			# clean the directories			
			else:
				for root, dirs, files in os.walk("nodes", topdown=False):
					# dont delete the files dir	
					split_path = root.split("/")			
					if split_path[-1] != "client":
						for f in files:
							os.remove(os.path.join(root,f))
		ixpfabric = self.addSwitch('sw1')
		for node in chordDFSHosts:
			self.addLink(node, ixpfabric)
