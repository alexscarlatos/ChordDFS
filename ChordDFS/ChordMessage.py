class ChordMessage():
	# Message types
	FIND_SUCCESSOR = "FIND_SUCCESSOR"           # Propogate a find successor message
	RETURN_SUCCESSOR = "RETURN_SUCCESSOR"       # Return the result of a find successor query
	GET_PREDECESSOR = "GET_PREDECESSOR"         # Request a node's predecessor
	RETURN_PREDECESSOR = "RETURN_PREDECESSOR"   # Return your predecessor
	NOTIFY_PREDECESSOR = "NOTIFY_PREDECESSOR"   # Notify node that you are its predecessor
	CHECK_ALIVE = "CHECK_ALIVE"          		# Request a validation that you are alive
	AM_ALIVE = "AM_ALIVE"              	    	# Return alive validation
	SEND_FILE = "SEND_FILE"             	    # Forward a file to a node
	REQUEST_FILE = "REQUEST_FILE"          	    # Request a file from a node (or client)
	SOMEONE_DIED = "SOMEONE_DIED"				# Inform that we are aware of a node that has died
	LEAVING = "LEAVING"							# Inform that we are leaving the network

	# Message types specific to Tracker/Client interactions
	INSERT_FILE = "INSERT"						# Insert a file
	GET_FILE = "GET"							# Get a file
	GET_FILE_LIST = "LIST"						# List available files	
	ERR = "ERR"									# Error
	SUCCESS = "SUCCESS"							# Successful transaction
	ENTRIES = "ENTRIES"							# Get entries node is responsible for

	# Network file operations
	OP_SEND_FILE = "SEND"
	OP_REQ_FILE = "REQUEST"
	OP_INSERT_FILE = "INSERT"

	# MISC
	EXIT = "EXIT"
	HELP = "HELP"

# default msg dictionary key:values
def newMsgDict():
	msg = dict()
	msg['msg_type'] = None
	msg['filename'] = None
	msg['finger'] = None
	msg["client_ip"] = None
	msg['suc_ip'] = None
	msg['key'] = None
	msg['target'] = None
	msg['pred_ip'] = None
	msg['content'] = None
	msg['hash'] = None
	msg['dead_node'] = None
	msg["file_list"] = None
	msg["hops"] = 0
	return msg