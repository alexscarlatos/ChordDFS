from datetime import datetime
import hashlib
import json
import os
import random
import select
import signal
import socket
import struct
import sys
import threading
import time
import os

from ReadLog import MyLogger
from ChordMessage import ChordMessage as c_msg
from ChordMessage import newMsgDict


# Represents any object that has a place on the Chord ring
class ChordNode:
    def __init__(self, key, name="", isFile=False):
        # Chord Nodes can be used for network nodes or files
        self.ip = key
        self.filename = key
        self.name = name
        
        # Use hash to find position on ring
        if isFile:
            self.chord_id = [h % ring_size for h in get_hash(key, numHashes=num_replicates)]
        else:
            self.chord_id = get_hash(key)[0] % ring_size

    def __str__(self):
        if self.name == "":
            return "key: {0}, chord id: {1}".format(self.ip, self.chord_id)
        return "key: {0}, name: {1}, chord id: {2}".format(self.ip, self.name, self.chord_id)

    def generate_fingers(self, finger_table_size):
        ''' Generate skeleton fingers
        '''
        fingers = []
        for index in range(finger_table_size):
            fingers.append(self.chord_id + (2**index))        
        return fingers    

    def print_finger_table(self, finger_table):
        ''' Print entries in finger table
        '''
        text = "\n"
        index = 0
        print(finger_table.keys())
        for key,value in sorted(finger_table.items()):
            if value is not None:
                text +="N{0} + {1}: {2}\n".format(key-(2**index),2**index,value.ip)
            else:
                text +="N{0} + {1}: {2}\n".format(key-(2**index),2**index,"None")
            index +=1
        return text

# Get the hash of a key
def get_hash(key, numHashes=1):
    hash_func = hashlib.sha1()
    hashList = []
    for i in range(numHashes):
        # Update with key and keep first 4 bytes
        hash_func.update(key.encode())
        hash_bytes = hash_func.digest()
        hashList.append(struct.unpack("<L", hash_bytes[:4])[0])

    return hashList

# Get the outcome of a random roll with probability p
def bernoulli(p):
    return random.uniform(0, 1) <= p

# Send a UDP message to another node
def sendCtrlMsg(dst_ip, msg_type, msg):
    # Include the type of message this is
    msg['msg_type'] = msg_type

    # Serialize the message
    msg_json = json.dumps(msg)
    if sys.version_info[0] >= 3:
        msg_json = bytes(msg_json, encoding="utf-8")
    
    # Send the message to the destination's control port
    control_sock.sendto(msg_json, (dst_ip, control_port))
    myLogger.mnPrint("msg type:{0} sent to {1}: msg:{2}".format(msg_type, dst_ip, myLogger.pretty_msg(msg)), debug=False)

# Received a UDP message
def ctrlMsgReceived():
    global successor, predecessor, entries, outstanding_file_reqs, finger_table, tracker_node_ip, inNetwork

    # Get data from socket
    try:
        data, addr = control_sock.recvfrom(1024)
    except socket.error as e:
        print(e)
        return
    
    # Drop all packets if we are not participating in the network
    if not inNetwork:
        return

    # Parse message type, update hops, and respond accordingly
    msg = json.loads(str(data))
    msg_type = msg['msg_type']
    msg["hops"] += 1
    myLogger.mnPrint("msg type:{0} rcvd from {1}: msg:{2}".format(msg_type, addr[0], myLogger.pretty_msg(msg)), debug=False)

    # We are supposed to find target's successor
    if msg_type == c_msg.FIND_SUCCESSOR:
        key = msg['key']
        target = msg['target']
        #filename = msg['filename']
        findSuccessor(key, target, msg)

    # Someone returned our find successor query
    elif msg_type == c_msg.RETURN_SUCCESSOR:
        suc_ip = msg['suc_ip']
        filename = msg['filename']
        finger = msg['finger']
        # No filename indicates we wanted to find our successor
        if filename is None:
	        # Finger update
            if finger is not None:
                finger_table[finger] = ChordNode(suc_ip)            
                #myLogger.mnPrint(me.print_finger_table(finger_table))
            # Successor update
            else:
                successor = ChordNode(suc_ip)
                myLogger.mnPrint("Successor updated by find successor: {0}".format(successor))
        # Filename indicates we wanted to find a file's location
        else:
            fileNode = ChordNode(filename, isFile=True)
            myLogger.mnPrint("Found file ({0}) ({1}) at ({2})".format(fileNode, msg['key'], ChordNode(suc_ip)))
            if outstanding_file_reqs[filename] == c_msg.OP_SEND_FILE:
                # When we get to this point, assume that file content is already present in msg
                sendFile(suc_ip, msg)
            elif outstanding_file_reqs[filename] == c_msg.OP_REQ_FILE:
                sendCtrlMsg(suc_ip, c_msg.REQUEST_FILE, msg)
            elif outstanding_file_reqs[filename] == c_msg.OP_INSERT_FILE:
                sendCtrlMsg(suc_ip, c_msg.INSERT_FILE, msg)

    # Someone wants to know who our predecessor is
    elif msg_type == c_msg.GET_PREDECESSOR:
        msg = newMsgDict()
        msg['pred_ip'] = None if predecessor is None else predecessor.ip
        sendCtrlMsg(addr[0], c_msg.RETURN_PREDECESSOR, msg)

    # Our successor told us who their predecessor is
    elif msg_type == c_msg.RETURN_PREDECESSOR:
        pred_ip = msg['pred_ip']
        stabilize(None if pred_ip == None else ChordNode(pred_ip))

    # Someone told us that they are our predecessor
    elif msg_type == c_msg.NOTIFY_PREDECESSOR:
        pred_ip = msg['pred_ip']
        if pred_ip is not None:
            notify(ChordNode(pred_ip))

    # Someone wants to know we are alive
    elif msg_type == c_msg.CHECK_ALIVE:
        sendCtrlMsg(addr[0], c_msg.AM_ALIVE, msg)

    # Someone told us they were alive
    elif msg_type == c_msg.AM_ALIVE:
        waiting_for_alive_resp[addr[0]] = False

    # Someone sent us a file
    elif msg_type == c_msg.SEND_FILE:
        filename = msg['filename']
        fileNode = ChordNode(filename, isFile=True)
        content = msg['content']

        '''
        # If this file was for a recovery, make sure it was meant for us, otherwise reinsert
        lost_key = msg['hash']
        if lost_key is not None and predecessor is not None:
            if not keyInRange(lost_key, predecessor.ip, me.ip, inc_end=True):
                myLogger.mnPrint("Received {0} to recover at {1}, but this doesn't belong to us".format(fileNode, lost_key))
                # Wait, and then restart request to recover file
                t = threading.Timer(2 * refresh_rate, lambda: sendCtrlMsg(tracker.ip, c_msg.SOMEONE_DIED, msg))
                t.start()
                return
        '''

        # Save this file
        with open("{0}/{1}".format(file_dir_path, filename), "w") as newFile:
            newFile.write(content)
        entries[filename] = fileNode
        myLogger.mnPrint("Received file " + filename + " from " + str(addr[0]))

        # If file from the client -> tell them insertion was successful
        if msg["client_ip"] != None:
            sendCtrlMsg(msg["client_ip"], c_msg.INSERT_FILE, msg)
        # Current responsible entries
        myLogger.mnPrint("entry keys: {0}".format(entries.keys()))

    # Someone wants a file from us
    elif msg_type == c_msg.REQUEST_FILE:
        # Send directly to client
        if msg["client_ip"] is not None:
            myLogger.mnPrint(msg['filename'] + " requested from " + msg["client_ip"])
            sendFile(msg["client_ip"], msg, readFromFile=True)            
        # Send to node who requested it
        # TODO: don't think this should happen
        else:
            sendFile(addr[0], msg, readFromFile=True, rmEntry=True)
            myLogger.mnPrint(msg['filename'] + " requested from " + addr[0])

    # We were informed of the death of a node
    elif msg_type == c_msg.SOMEONE_DIED:
        dead_node = ChordNode(msg['dead_node'])
        dn_pred = ChordNode(msg['pred_ip'])
        myLogger.mnPrint("Heard that {0} died".format(dead_node))
        
        # For every file in the network...
        for f, cn in allFiles.items():
            # See if a copy of it should have been in dn
            keys_not_in_dn = []
            dn_fkey = 0
            for k in cn.chord_id:
                if keyInRange(k, dn_pred.chord_id, dead_node.chord_id, inc_end=True):
                    dn_fkey = k
                else:
                    keys_not_in_dn.append(k)
            
            # If all copies were there, there is no chance of recovery and the file is lost
            if len(keys_not_in_dn) == 0:
                pass # TODO: mark file as lost
            # If at least one copy was there, find another node that is hosting the file
            elif len(keys_not_in_dn) < num_replicates:
                myLogger.mnPrint("Attempting to re-insert {0} ({1}) from {2}".format(f, dn_fkey, keys_not_in_dn[0]))
                outstanding_file_reqs[f] = c_msg.OP_INSERT_FILE
                msg['filename'] = f
                msg['hash'] = dn_fkey
                findSuccessor(keys_not_in_dn[0], me.ip, msg)

    # We were informed that a node is leaving
    elif msg_type == c_msg.LEAVING:
        suc_ip = msg['suc_ip']
        pred_ip = msg['pred_ip']
        if suc_ip is not None:
            successor = ChordNode(suc_ip)
            myLogger.mnPrint("Successor updated by prev leaving: {0}".format(successor))
        elif pred_ip is not None:
            predecessor = ChordNode(pred_ip)
            myLogger.mnPrint("Predecessor updated by prev leaving: {0}".format(predecessor))

    # We are supposed to insert a file into the network
    elif msg_type == c_msg.INSERT_FILE:
        filename = msg['filename']
        outstanding_file_reqs[filename] = c_msg.OP_SEND_FILE
        fileNode = ChordNode(filename, isFile=True)
        
        # If from client, we are inserting a file for the first time
        if msg['client_ip'] is not None:
            myLogger.mnPrint("Inserting " + str(fileNode) + " into the network")
            allFiles[filename] = fileNode # TODO: only set on confirm? or should we assume this always succeeds?
            for chord_id in fileNode.chord_id:
                findSuccessor(chord_id, me.ip, msg)
        # Otherwise, we are reinserting a file that we think was partially lost
        else:
            # Find the new location of the lost key for reinsertion
            if filename in entries:
                lost_key = msg['hash']
                myLogger.mnPrint("Reinserting {0} ({1}) into the network".format(fileNode, lost_key))
                with open(file_dir_path + filename) as f:
                    msg['content'] = f.read()
                findSuccessor(lost_key, me.ip, msg)
            # If our network hasn't stabilized yet, we may have falsely received this request
            else:
                # Wait, and then restart request to recover file
                t = threading.Timer(2 * refresh_rate, lambda: sendCtrlMsg(tracker.ip, c_msg.SOMEONE_DIED, msg))
                t.start()

    # We are supposed to retrieve a file from the network
    elif msg_type == c_msg.GET_FILE:
        filename = msg['filename']
        outstanding_file_reqs[filename] = c_msg.OP_REQ_FILE
        fileNode = ChordNode(filename, isFile=True)
        myLogger.mnPrint("Retrieving " + str(fileNode))
        for k in fileNode.chord_id:
            findSuccessor(k, me.ip, msg)

    # send all known entries back to client if tracker
    elif msg_type == c_msg.GET_FILE_LIST:
        if me.ip == tracker_node_ip:
            msg["file_list"] = allFiles.keys()
            sendCtrlMsg(msg["client_ip"], c_msg.GET_FILE_LIST, msg)
    # avgs keys per node
    elif msg_type == c_msg.ENTRIES:
        # log entries and pass request along        
        myLogger.mnPrint("entries: {0}".format(print_entries()))
        # we've come full circle -> tell client about success
        if successor.ip == tracker_node_ip or\
           (successor.ip == tracker_node_ip and successor.ip == me.ip):            
            sendCtrlMsg(msg["client_ip"], c_msg.ENTRIES, msg)
        else:
            myLogger.mnPrint("Sending entries request forward: {0}".format(successor.ip))
            sendCtrlMsg(successor.ip, c_msg.ENTRIES, msg)

    # TODO: when will this happen?
    elif msg_type == c_msg.ERR:
        pass

# This calls all methods that need to be called frequently to keep the network synchronized
def refresh():
    global successor, predecessor, refresh_rate, inNetwork

    while True:
        if successor != None:
            # If we were waiting on a response from our successor and never got one, assume they died
            if waitingForAlive(successor.ip):
                # Inform the tracker that this node is dead so we can recover any files it was hosting
                msg = newMsgDict()
                msg['dead_node'] = successor.ip
                msg['pred_ip'] = me.ip
                sendCtrlMsg(tracker.ip, c_msg.SOMEONE_DIED, msg)

                myLogger.mnPrint("Our successor {0} has died!".format(successor))
                waiting_for_alive_resp[successor.ip] = False
                successor = None
                findSuccessor(me.chord_id, me.ip)
                successor = me
            # Will get our successor's predecessor and call stabilize on return
            else:
                waiting_for_alive_resp[successor.ip] = True
                sendCtrlMsg(successor.ip, c_msg.GET_PREDECESSOR, newMsgDict())

        # Update our finger table
        if using_finger_table and inNetwork:
            fixFingers()

        # Handle predecessor death
        if predecessor != None:
            # If we were waiting on a response from our predecessor and never got one, assume they died
            if waitingForAlive(predecessor.ip) and predecessor.ip is not successor.ip:
                myLogger.mnPrint("Our predecessor {0} has died!".format(predecessor))
                waiting_for_alive_resp[predecessor.ip] = False
                predecessor = None
            else:
                # Check to see if the predecessor is still alive
                waiting_for_alive_resp[predecessor.ip] = True
                sendCtrlMsg(predecessor.ip, c_msg.CHECK_ALIVE, newMsgDict())
                # Send any files that shouldn't be here to the predecessor
                sendFilesToPred()

        # Nodes should randomly leave/join the network or fail outright
        if not is_tracker:
            if bernoulli(leave_join_prob):
                if inNetwork:
                    leave()
                else:
                    join()
            if bernoulli(fail_prob):
                fail()

        # Wait for short time
        time.sleep(refresh_rate)

# Return if we are waiting for an alive response from the given ip
def waitingForAlive(ip):
    return ip in waiting_for_alive_resp and waiting_for_alive_resp[ip]

# Determine if the given key is between the two given endpoints
def keyInRange(key, start_id, end_id, inc_end=False):
    # If endpoints are on same side of chord ring
    if end_id > start_id:
        return key > start_id and (key <= end_id if inc_end else key < end_id)
    # If endpoints straddle the 0 point of the chord ring (or are equal)
    else:
        return key > start_id or (key <= end_id if inc_end else key < end_id)

# Join the network by finding out who your successor is
def join():
    global inNetwork

    inNetwork = True
    myLogger.mnPrint("Joining the network...")
    findSuccessor(me.chord_id, me.ip)

# Leave the network gracefully
def leave():
    global inNetwork, predecessor, successor

    inNetwork = False
    myLogger.mnPrint("Leaving the network...")

    # Send all of our current files to our successor
    if successor is not None:
        msg = newMsgDict()
        for f in list(entries.keys()):
            msg['filename'] = f
            sendFile(successor.ip, msg, readFromFile=True, rmEntry=True)

    if successor is not None and predecessor is not None:
        # Tell our successor we are leaving and pass them our predecessor
        msg = newMsgDict()
        msg['pred_ip'] = predecessor.ip
        sendCtrlMsg(successor.ip, c_msg.LEAVING, msg)

        # Tell our predecessor we are leaving and pass them our successor
        msg = newMsgDict()
        msg['suc_ip'] = successor.ip
        sendCtrlMsg(predecessor.ip, c_msg.LEAVING, msg)

    successor = None
    predecessor = None

# Simulate this node crashing
def fail():
    global inNetwork, predecessor, successor
    
    myLogger.mnPrint("Failing...")

    inNetwork = False
    predecessor = None
    successor = None

# Find the ip of the chord node that should succeed the given key
# If filename is specified, this is for finding a file location
def findSuccessor(key, target, msg=None):
    global successor

    # If key is somewhere between self and self.successor, then self.successor directly succeeds key
    if successor != None and keyInRange(key, me.chord_id, successor.chord_id, inc_end=True):
        # Build and send response
        if msg is None:
            msg = newMsgDict()                       
        msg['suc_ip'] = successor.ip
        sendCtrlMsg(target, c_msg.RETURN_SUCCESSOR, msg)
    # Otherwise, send request to successor
    else:
        # Get node to send request to
        if successor == None:
            dst = tracker
        elif using_finger_table:
            dst = closestPreceedingNode(key) # TODO: if dst == me, we might have a problem
        else:
            dst = successor

        # Build and send request
        if msg is None:
            msg = newMsgDict()       
        msg['key'] = key
        msg['target'] = target
        sendCtrlMsg(dst.ip, c_msg.FIND_SUCCESSOR, msg)

# Find the node in { {me} U finger_table } that preceeds the given key closest
def closestPreceedingNode(key):
    global finger_table, finger_table_size

    # Starting at furthest point in table, moving closer, see if table entry preceeds the given key
    for i in range(finger_table_size):
        if keyInRange(finger_table[fingers[i]], me.chord_id, key):
            return finger_table[fingers[i]]
    
    # Otherwise, we are the closest node we know of
    return me

# Given the returned predecessor of our successor, update if necessary and touch base with successor
def stabilize(x):
    global successor

    if successor is None:
        return

    waiting_for_alive_resp[successor.ip] = False

    # If x is closer than our current successor, it is our new successor
    if x != None and keyInRange(x.chord_id, me.chord_id, successor.chord_id):
        successor = x
        waiting_for_alive_resp[successor.ip] = False
        myLogger.mnPrint("Successor updated by stabilize: " + str(successor))

    # Notify successor that we are its predecessor
    msg = newMsgDict()
    msg['pred_ip'] = me.ip
    sendCtrlMsg(successor.ip, c_msg.NOTIFY_PREDECESSOR, msg)

# Send all necessary files to our predecessor
def sendFilesToPred():
    for f, cn in list(entries.items()):
        # For each chord_id for this file
        k_count = 0
        for k in cn.chord_id:
            # Count if this key should belong to us
            if keyInRange(k, predecessor.chord_id, me.chord_id, inc_end=True):
                k_count += 1

        # Send file to predecessor if none of the keys were for us
        if k_count == 0:
            myLogger.mnPrint("Transferring file ({0}) to predecessor ({1}) from file balancing".format(cn, predecessor))
            msg = newMsgDict()
            msg['filename'] = f
            sendFile(predecessor.ip, msg, readFromFile=True, rmEntry=True)

# Node told us that it is our predecessor
def notify(node):
    global predecessor

    # If the given id is between our current predecessor and us (or if we had no predecessor)
    #   then set it to be our predecessor
    if predecessor == None or keyInRange(node.chord_id, predecessor.chord_id, me.chord_id):
        myLogger.mnPrint("Predecessor updated by notify: " + str(node))

        # Transfer all necessary files to predecessor
        for f, cn in list(entries.items()):
            # For each chord_id for this file
            k_count = 0
            for k in cn.chord_id:
                # Count if this key should move to the new predecessor
                if predecessor is None:
                    if keyInRange(k, me.chord_id, node.chord_id, inc_end=True):
                        k_count += 1
                elif keyInRange(k, predecessor.chord_id, node.chord_id, inc_end=True):
                    k_count += 1

            # Send file to predecessor if necessary, rm this entry if all keys should go to predecessor
            if k_count > 0:
                myLogger.mnPrint("Transferring file ({0}) to new predecessor ({1}) from notify".format(cn, node))
                msg = newMsgDict()
                msg['filename'] = f
                sendFile(node.ip, msg, readFromFile=True, rmEntry=(k_count==num_replicates))

        predecessor = node
        waiting_for_alive_resp[predecessor.ip] = False

def fixFingers():
    '''Refresh the finger table entries periodicially'''
    global finger_table, finger_table_size
   
    for key in finger_table.keys():
        msg = newMsgDict()
        msg["finger"] = key
        findSuccessor(key, me.ip, msg)

# Send a file to a node
def sendFile(dst_ip, msg, readFromFile=False, rmEntry=False):    
    filename = msg['filename']
    if readFromFile:
        try:
            with open(file_dir_path + filename) as f:
                msg['content'] = f.read()
        except IOError as e:
            sendCtrlMsg(dst_ip, c_msg.ERR, msg)
            myLogger.mnPrint("Error: {0} not found!".format(filename))
            myLogger.mnPrint(e)
            return
    if rmEntry:
        if filename in entries:
            del entries[filename]
            os.remove(file_dir_path + filename)
        else:
            mnPrint(filename + " not found in entries")
    myLogger.mnPrint("Sending " + filename + " to " + dst_ip)
    sendCtrlMsg(dst_ip, c_msg.SEND_FILE, msg)

def print_entries():
    global entries
    if len(entries.keys()) == 0:
        return "{}"
    entries_str = "{"
    for key,value in entries.items():
        entries_str += "{0}:{1};".format(key,value.chord_id)
    entries_str = entries_str[:-1] + "}"
    return entries_str

def exit(arg=None):
    '''exit the application
    '''
    if arg is not None:
        sys.exit(arg)
    sys.exit()     

if __name__ == "__main__":
    # Default parameters
    finger_table_size = 6
    tracker_node_ip = "172.1.1.1"
    control_port = 500
    using_finger_table = False
    num_replicates = 1
    refresh_rate = 1
    leave_join_prob = 0
    fail_prob = 0

    try:
        # Open config file
        configFile = open("chordDFS.config")
        config = json.loads(configFile.read())
        configFile.close()

        # Load parameters from config file
        finger_table_size = config['finger_table_size']
        tracker_node_ip = config['tracker_node_ip']
        control_port = config['control_port']
        using_finger_table = config['using_finger_table']
        num_replicates = config['num_replicates']
        refresh_rate = config['refresh_rate']
        leave_join_prob = config['leave_join_prob']
        fail_prob = config['fail_prob']
    except:
        pass

    # Ring size is relative to finger table size s.t.
    #   the last entry on the finger table will cross half the ring
    ring_size = 2**finger_table_size # m

    # Pass in self as ip (getpeername gets localhost as ip)
    my_ip = ""
    my_name = ""
    if len(sys.argv) < 3:
        myLogger.mnPrint("Missing self ip and name!")
        exit()
    my_ip = sys.argv[1]
    my_name = sys.argv[2]
    me = ChordNode(my_ip, name=my_name)

    # Set relative file paths
    log_file_path = "nodes/{0}/logs/{1}.log".format(me.name, me.ip.replace(".", "_"))
    node_directory = "nodes/" + me.name    
    file_dir_path = node_directory + "/files/chord/"    

    # Get tracker based on ip from config
    tracker = ChordNode(tracker_node_ip)

    # If we are the tracker node
    is_tracker = me.ip == tracker_node_ip

    # create logger
    myLogger = MyLogger(me.ip, me.chord_id, log_file_path)

    # Announce initialization
    myLogger.mnPrint("Hi! I'm a chord node, my IP is {0}, my chord_id is {1}, my name is {2}".format(me.ip, me.chord_id, me.name))
    if is_tracker:
        myLogger.mnPrint("Oh, and I'm the tracker!")
    
    # Socket specifically for communicating with other chord nodes
    control_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    control_sock.bind((me.ip, control_port))

    # Every file that we are responsible for (name->ChordNode)
    entries = dict()

    # Every file on the network, only used by tracker (name->ChordNode)
    allFiles = dict()

    # Maps filename to operation we want to perform when we find its location in the ring ('send' or 'request' or 'insert')
    outstanding_file_reqs = dict()

    # If we are waiting for a certain node to tell us that it is alive
    waiting_for_alive_resp = dict()

    # Predecessor is null by default
    predecessor = None

    # Tracker creates the network, and is thus its own successor
    if is_tracker:
        inNetwork = True
        successor = me
    # Every other node is joining the network after the tracker
    else:
        time.sleep(1)
        inNetwork = False
        successor = None
        # We want most nodes to join the network initially
        if bernoulli(1 - leave_join_prob):
            join()

    # up to m entries; me.name + 2^i
    if using_finger_table:
        fingers = me.generate_fingers(finger_table_size)
        finger_table = {key: None for key in fingers}
        fixFingers()    

    # Install timer to run processes
    timer = threading.Thread(target=refresh)
    timer.start()

    # Multiplexing lists
    rlist = [control_sock]
    wlist = []
    xlist = []

    while True:
        # Multiplex on possible network messages
        try:
            _rlist, _wlist, _xlist = select.select(rlist, wlist, xlist)
        except:
            continue

        if control_sock in _rlist:
            ctrlMsgReceived()
