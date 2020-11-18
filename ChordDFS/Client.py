import time
import json
import socket
import select
import sys
import os
import fcntl
import threading

from ReadLog import MyLogger
from ChordMessage import ChordMessage as c_msg
from ChordMessage import newMsgDict

class Client():
    '''
    Client class, used to communicate with the Chord servers
    ip: ip of the client
    name: name of the client    
    '''
    def __init__(self, ip, name, control_sock):
        # Chord Nodes can be used for network nodes or files
        self.ip = ip        
        self.name = name
        self.last_request = None
        self.control_sock = control_sock

        # Default parameters    
        self.tracker_node_ip = "172.1.1.1"
        self.control_port = 500
        self.retries = 0

        try:
            # Open config file
            configFile = open("chordDFS.config")
            config = json.loads(configFile.read())
            configFile.close()

            # Load parameters from config file        
            self.tracker_node_ip = config['tracker_node_ip']
            self.control_port = config['control_port']
            self.rate = config['client_rate']

        except:
            pass        
        # file directory
        self.file_dir_path = "nodes/{0}/files/client/".format(self.name)    
        # logging
        log_file_path = "nodes/{0}/logs/{1}_c.log".format(self.name, self.ip.replace(".", "_"))
        # create logger
        self.myLogger = MyLogger(self.ip, self.name, log_file_path, client=True)

        # Announce initialization
        self.myLogger.mnPrint("Hi! I'm a chord client, my IP is {0}".format(self.ip, self.name))        

    def __str__(self):
        return "ip: {0}, name: {1}\nlast: {2}".format(self.ip, self.name, self.last_request)

    '''Main Methods'''
    def insert_file(self, filename):
        ''' Insert a file
        '''
        self.last_request = [c_msg.INSERT_FILE, filename]
        try:
            with open(self.file_dir_path+filename) as f_in:
                content = f_in.read()
                msg = newMsgDict()
                msg['filename'] = filename
                msg['content'] = content
                msg['client_ip'] = self.ip
                msg["hops"] = 0
                self.sendMessage(c_msg.INSERT_FILE, msg)                
        except IOError as e:
            self.myLogger.mnPrint("Error: last request:{0} failed!".format(self.last_request))
            self.myLogger.mnPrint(e)

    def get_file(self, filename):
        '''Request a file
        '''
        self.last_request = [c_msg.GET_FILE,filename]    
        msg = newMsgDict()
        msg['filename'] = filename
        msg["client_ip"] = self.ip
        msg["hops"] = 0
        self.sendMessage(c_msg.GET_FILE, msg)    
        

    def get_file_list(self):
        '''Request available files
        '''
        self.last_request = [c_msg.GET_FILE_LIST]    
        msg = newMsgDict()        
        msg["client_ip"] = self.ip
        msg["hops"] = 0
        self.sendMessage(c_msg.GET_FILE_LIST, msg)

    def entries(self):
        '''Tell server to write entries to log
        '''
        self.last_request = [c_msg.ENTRIES]    
        msg = newMsgDict()        
        msg["client_ip"] = self.ip
        msg["hops"] = 0
        self.sendMessage(c_msg.ENTRIES, msg)
        

    '''Helper methods'''
    def processRequest(self, request, args=None):
        '''
        process a request
        request: the request to process
        arg: arg for request
        '''
        self.myLogger.mnPrint("received request: {0}:{1}".format(request, args))        
        if request == c_msg.GET_FILE:
            self.get_file(args[0])
        elif request == c_msg.INSERT_FILE:
            self.insert_file(args[0])            
        elif request == c_msg.GET_FILE_LIST:
            self.get_file_list()
        elif request == c_msg.ENTRIES:
            self.entries()            
        elif request == "LS":
            self.list_dir()        

    def sendMessage(self, msg_type, msg):
        '''Send message to tracker node
        '''
        # Include the type of message this is
        msg['msg_type'] = msg_type

        # Serialize the message
        msg_json = json.dumps(msg)
        if sys.version_info[0] >= 3:
            msg_json = bytes(msg_json, encoding="utf-8")

        # Send the message to the destination's control port
        self.control_sock.sendto(msg_json, (self.tracker_node_ip, self.control_port))
        self.myLogger.mnPrint("msg type:{0} sent to {1}: msg:{2}".format(msg_type, self.tracker_node_ip, self.myLogger.pretty_msg(msg)))

    def list_dir(self):
        '''
        list own directory
        '''        
        files = os.listdir(self.file_dir_path)        
        for f in files:
            print(f)
        sys.stdout.flush()

    def processResponse(self, data, addr):
        msg = json.loads(str(data))
        msg_type = msg['msg_type']
        msg["hops"] += 1
        self.myLogger.mnPrint("msg type:{0} rcvd from {1}: msg:{2}".format(msg_type, addr[0], self.myLogger.pretty_msg(msg)))
        # file from server
        if msg_type == c_msg.SEND_FILE:
            filename = msg["filename"]
            content = msg["content"]
            with open(self.file_dir_path+filename, "w") as newFile:
                newFile.write(content)
            self.myLogger.mnPrint("Received file " + filename + " from " + str(addr[0]))
            #self.list_dir()
        # success for last request
        if msg_type == c_msg.INSERT_FILE or msg_type == c_msg.ENTRIES:
            self.myLogger.mnPrint("Success: last request:{0} succeeded!".format(self.last_request))            
        # error for last request
        if msg_type == c_msg.ERR:
            self.myLogger.mnPrint("Error: last request:{0} failed!".format(self.last_request))                
            # retry 3x then give up        
            if self.retries < 3:
                self.retries +=1
                self.myLogger.mnPrint("Retrying attempt: {0}".format(self.retries))
                self.processRequest(self.last_request[0], self.last_request[-1])
        if msg_type == c_msg.GET_FILE_LIST:
            self.myLogger.mnPrint("Server Files:\n{0}".format(self.print_dir(msg["file_list"])))

    def print_dir(self, dir):
        str = ""
        for f in dir:
            str += "{0}\n".format(f)    
        return str[:-1]    

'''utility functions'''
def exit(arg=None):
    '''exit the application
    '''
    if arg is not None:
        sys.exit(arg)
    sys.exit()

def ctrlMsgReceived():
    '''Handle received a msg from control socket'''
    global me
    # Get data from socket
    #me.control_sock.settimeout(me.rate)
    try:
        data, addr = me.control_sock.recvfrom(1024)
    except socket.error as e:
        print(e)
        return
    # Parse message type and respond accordingly        
    me.processResponse(data,addr)

def receiveMessages():
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

def processStdin():
    '''Process the stdin input and take appropriate action
    '''
    global me
    global std_input
    # read until new line
    ch = sys.stdin.read()
    if ch != "\n":
        std_input += ch
        return
    else:
        args = std_input.split(" ")
        std_input = ""
    # handle stdin
    if len(args) > 0:
        me.myLogger.mnPrint("received stdin request: {0}".format(args))
        cmd = args[0].upper().strip()        
        # HELP
        if cmd == c_msg.HELP:
            help()    
        # EXIT
        elif cmd == c_msg.EXIT:
            exit()
        # client handles itself
        else:    
            me.processRequest(cmd, args[1:])

def help():
    '''
    Prints the help menu
    '''
    help_str = '''Chord Client Application v1.0 
    insert file    insert file into Chord Ring
    get file       get file from Chord Ring
    list           list all available files in Chord Ring
    exit           exit application
    help           print help screen
    '''
    print(help_str)
    sys.stdout.flush()

if __name__ == "__main__":    

    # Pass in self as ip
    my_ip = ""
    my_name = ""
    if len(sys.argv) < 3:
        print("Missing self ip and name!")
        sys.stdout.flush()
        sys.exit()
    my_ip = sys.argv[1]
    my_name = sys.argv[2]    

    # stdin interaction    
    std_input = ""

    # script testing        
    script = None    
    if len(sys.argv) == 4:
        script = sys.argv[3]
        
    # Socket specifically for communicating with other chord nodes
    control_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # set up client
    me = Client(my_ip, my_name, control_sock)    

    control_sock.bind((me.ip, me.control_port))          

    # script --> blocking
    if script is not None:
        timer = threading.Thread(target=receiveMessages)
        timer.start()
        cmds_to_run = []
        with open(script, "r") as f_in:
            cmds_to_run = f_in.read().strip().split("\n")
        while len(cmds_to_run) != 0:
            args = cmds_to_run.pop(0).split(" ")
            if len(args) != 0 and args[0] != "":
                cmd = args[0].upper().strip()                    
                me.processRequest(cmd, args[1:])
                #ctrlMsgReceived()
            time.sleep(me.rate)
        # prevent broken pipe
        exit()

    # Multiplexing lists
    fcntl.fcntl(sys.stdin, fcntl.F_SETFL, fcntl.fcntl(sys.stdin, fcntl.F_GETFL) | os.O_NONBLOCK)    
    rlist = [control_sock, sys.stdin]
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

        if sys.stdin in _rlist:
            processStdin()            
