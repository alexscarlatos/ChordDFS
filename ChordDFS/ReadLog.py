import os
import json
import sys
import re
from ChordMessage import ChordMessage as c_msg
from datetime import datetime

class MyLogger():
    def __init__(self, ip, chord_id, log_file_path, client=False):     
        self.ip = ip   
        self.chord_id = chord_id
        self.log_file_path = log_file_path
        self.client = client

    # Print that will show up in mininet output and get added to log file
    def mnPrint(self, msg, debug=True):       
        # log only certain message types
        try:
            # filter msg types
            messages = [c_msg.FIND_SUCCESSOR,c_msg.RETURN_SUCCESSOR,c_msg.GET_PREDECESSOR,c_msg.RETURN_PREDECESSOR,c_msg.NOTIFY_PREDECESSOR,c_msg.CHECK_ALIVE,c_msg.AM_ALIVE,c_msg.SOMEONE_DIED,\
                            c_msg.LEAVING]
            for msg_type in messages:
                if msg.find(msg_type) > 0:
                    return            
        except:
            pass     
        if self.client:
            # Format msg
            msg = "<{0}_c>: {1}".format(self.ip, msg)
        else:
            # Format msg
            msg = "<{0}, {1}>: {2}".format(self.ip, self.chord_id, msg)

        # Print msg to stdout 
        if debug:  
            print(msg)
            sys.stdout.flush() # need to flush output, else never show up

        # Write msg to log file        
        with open(self.log_file_path, "a") as logFile:
            logFile.write("{0} {1}\n".format(str(datetime.now()).replace(" ", "_"), msg))

    def pretty_msg(self, msg):
        '''Only print key,value pairs where value is not None'''
        pretty = "{"
        for key, value in msg.items():
            if value is not None:
                pretty += "{0}:{1},".format(key,value)
        pretty = pretty[:-1] + "}"
        return pretty

# functions for main application
def help():
    help_str = '''Chord Log Application v1.0 
    ring           print chord ring    
    stabilize      get stabilization time
    start          application start time
    end            application end time
    servers        number of server nodes
    clients        number of client nodes
    exit           exit application
    help           print help screen
    '''

def ring():
    global log_str
    chord_ring = ""
    # find chord ids
    ring_re = re.compile(r"chord_id is [0-9]+")
    # get chord ids
    num_re = re.compile(r'[0-9]+')
    # sort ids
    nodes = sorted(list(map(int,num_re.findall("".join(ring_re.findall(log_str))))))
    for node in nodes:
        chord_ring += "{0}->".format(node)
    chord_ring += str(nodes[0])
    return chord_ring

def start():
    global sorted_entries
    return sorted_entries[0]["time"]

def end():
    global sorted_entries
    return sorted_entries[-1]["time"]
    
def report():
    global log_str
    # report of log summaries etc
    inserts_str = inserts()
    gets_str = gets()
    keys_tup = keys()        
    key_summary_str = key_summary(keys_tup[1])
    report_str = \
    '''
    Start: {0}\n\
    End: {1}\n\
    # Servers: {2}\n\
    # Clients: {3}\n\
    Ring: {4}\n\
    Stabilization Time: {5}\n\
    Inserts Sent: {6}\n\
    Inserts Rcvd: {7}\n\
    Inserts Avg Hops: {8}\n\
    Insert Loss Rate: {9}\n\
    Gets Sent: {10}\n\
    Gets Rcvd: {11}\n\
    Gets Avg Hops: {12}\n\
    Gets Loss Rate: {13}
    Keys Dist:\n{14}\n\
    Key Summary: {15}\n\
    '''.format(start(),end(),servers(),clients(),ring(),stabilize(),\
        inserts_str[0],inserts_str[1],inserts_str[2],inserts_str[3],\
        gets_str[0],gets_str[1],gets_str[2],gets_str[3],\
        keys_tup[0],key_summary_str)
    return report_str

def stabilize():
    global log_str
    # example 2018-05-09_10:40:34.210800 <172.1.1.3, 11>: Successor updated by stabilize: key: 172.1.1.4, chord id: 47
    stabilize_re = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{6} <[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+, [0-9]+>: Successor updated by stabilize")    
    times_stab = stabilize_re.findall(log_str)
    time_re = re.compile(r"[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{6}")    
    times = time_re.findall("".join(times_stab))    
    start = datetime.strptime(times[0],"%H:%M:%S.%f")
    end = datetime.strptime(times[-1],"%H:%M:%S.%f")
    total = end - start
    final = "{0} sec".format(total.total_seconds())
    return final

def servers():
    global log_str    
    # find chord ids
    ring_re = re.compile(r"chord_id is [0-9]+")
    nodes = ring_re.findall(log_str)
    return len(nodes)

def clients():
    global log_str    
    # find clients    
    # ex: I'm a chord client, my IP is 172.1.1.2
    ring_re = re.compile(r"chord client, my IP is [0-9]+\.[0-9]+\.[0-9]+\.[0-9]+")
    ip_re = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+")    
    nodes = ip_re.findall("".join(ring_re.findall(log_str)))
    return len(nodes)

def inserts():
    global log_str, num_replicates
    # example <172.1.1.2_c>: msg type:INSERT sent to 172.1.1.1:
    '''2018-05-09_10:40:36.868994 <172.1.1.2_c>: msg type:INSERT rcvd from 172.1.1.1: msg:{client_ip:172.1.1.2,target:172.1.1.1,msg_type:INSERT,hops:7,filename:temp.txt,content:testingggg,suc_ip:172.1.1.1,key:5}'''
    insert_sent_re = re.compile(r"<[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+_c>: msg type:INSERT sent to 172.1.1.1")
    insert_sent = insert_sent_re.findall(log_str)
    num_inserts_sent = len(insert_sent)
    insert_re = re.compile(r"<[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+_c>: msg type:INSERT rcvd .*hops:[0-9]+")
    inserts_arr = insert_re.findall(log_str)
    num_inserts = int(len(inserts_arr)/num_replicates)
    hops_re = re.compile(r"hops:[0-9]+")
    hops = hops_re.findall("".join(inserts_arr))
    num_re = re.compile(r'[0-9]+')
    hops_nums = list(map(int,num_re.findall("".join(hops))))
    if len(hops_nums) == 0:
        avg_hops = 0
    else:
        avg_hops = sum(hops_nums)/len(hops_nums)
    loss_rate = 0
    if num_inserts_sent != 0:
        loss_rate = 1 - (num_inserts/num_inserts_sent)      
    inserts_str = (num_inserts_sent,num_inserts,avg_hops,loss_rate)
    return inserts_str

def gets():
    global log_str
    '''2018-05-09_10:41:42.752116 <172.1.1.2_c>: msg type:SEND_FILE rcvd from 172.1.1.1: msg:{client_ip:172.1.1.2,target:172.1.1.1,msg_type:SEND_FILE,hops:7,filename:temp.txt,content:testingggg,suc_ip:172.1.1.1,key:5}'''    
    get_sent_re = re.compile(r"<[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+_c>: msg type:SEND_FILE sent")
    get_re = re.compile(r"<[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+_c>: msg type:SEND_FILE rcvd .*hops:[0-9]+")
    get_sent = get_sent_re.findall(log_str)
    num_get_sent = len(get_sent)
    gets_arr = get_re.findall(log_str)
    num_gets = int(len(gets_arr)/num_replicates)
    hops_re = re.compile(r"hops:[0-9]+")
    hops = hops_re.findall("".join(gets_arr))
    num_re = re.compile(r'[0-9]+')
    hops_nums = list(map(int,num_re.findall("".join(hops))))
    if len(hops_nums) == 0:
        avg_hops = 0
    else:
        avg_hops = sum(hops_nums)/len(hops_nums)
    loss_rate = 0
    if num_get_sent != 0:
        loss_rate = 1 - num_gets/num_get_sent    
    gets_str = (num_get_sent,num_gets,avg_hops,loss_rate)
    return gets_str

def keys():
    '''
    2018-05-09_15:35:05.715598 <172.1.1.3, 11>: entries: {}
    2018-05-09_15:35:05.717302 <172.1.1.5, 35>: entries: {text.text:[33],newfile:[34]}
    '''
    entries_re = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{6} <[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+, [0-9]+>: entries: {.*}")
    entries = entries_re.findall(log_str)    
    dictionary_re = re.compile(r"{.*}")
    key_map = {}
    if len(entries) == 0:
        return "None", {}
    for entry in entries:
        ipid_re = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+, [0-9]+")
        time_re = re.compile(r"[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{6}")
        ip = ipid_re.findall(entry)[0]
        ts = datetime.strptime(time_re.findall(entry)[0],"%H:%M:%S.%f")
        n_entries = dictionary_re.findall(entry)        
        # only use most up to date info
        if ip in key_map:
            if key_map[ip]["timestamp"] < ts:
                key_map[ip]["timestamp"] = ts
                key_map[ip]["entries"] = n_entries
        else:
            key_map[ip] = {"timestamp":ts,"entries":n_entries}       
    return print_key_map(key_map), key_map

def print_key_map(key_map):
    key_map_str = "\t"
    for key in key_map.keys():        
        key_map_str += "{0}:{1}\n\t".format(key,print_list(key_map[key]["entries"]))
    return key_map_str        

def print_list(some_list):
    list_str = ""
    for value in some_list:
        list_str += value + " "
    return list_str

def key_summary(key_map):
    file_set = set()   
    key_map_str = ""
    for key in key_map.keys():
        entries = key_map[key]["entries"][0].replace("}","").replace("{","").split(";")
        empty = 0
        for entry in entries:            
            filename = entry.split(":")[0]
            if filename != "":                            
                file_set.add(filename)
            else:
                empty += 1
        key_map[key]["num_entries"] = len(entries) - empty
    for key in key_map.keys():
        key_map_str += "{0}-> # keys:{1}\n\t".format(key,key_map[key]["num_entries"])
    key_map_str_head = "\n\tTotal Files: {0}\n\t".format(len(file_set))    
    return key_map_str_head + key_map_str


if __name__ == "__main__":       
    # Get every file in logs folder
    logFileNames = []
    for root, dirs, files in os.walk("nodes", topdown=False):
        for f in files:
            if f.endswith(".log"):
                logFileNames.append(os.path.join(root, f))               
    # Get all entries from log files
    entries = []
    for logFileName in logFileNames:
        logFile = open(logFileName)
        for line in logFile:
            timestamp = line.strip().split(" ", 1)[0]
            # skip non timestamps
            try:
                timestamp = datetime.strptime(timestamp, "%Y-%m-%d_%H:%M:%S.%f")
            except:
                continue
            entry = dict()
            entry['time'] = timestamp
            entry['log'] = line
            entries.append(entry)
        logFile.close()     
    log_str = ""
    sorted_entries = sorted(entries, key=lambda e: e['time'])
    # Print all entries in order
    update = 0
    iter = 0
    for entry in sorted_entries:
        update += 1
        if update == 1000:
            iter += 1
            print("iter: {0}x1000".format(iter))
            update = 0
        log_str += entry['log']        

    # same compiled logs into 1  
    try:  
        with open("master.log", "w") as f_out:
            f_out.write(log_str)  
    except:
        pass        
    input_str = ""    

    try:
        # Open config file
        configFile = open("chordDFS.config")
        config = json.loads(configFile.read())
        configFile.close()

        # Load parameters from config file        
        num_replicates = config['num_replicates']
    except IOError as e:
        print(e)

    # just run the report
    if len(sys.argv) > 1:        
        print(report())
        sys.stdout.flush()
        sys.exit(1)

    while True:
        input_str = input("Enter a command: ")
        sys.stdout.flush()

        if input_str == "help":
            help()
        if input_str == "exit":
            break
        if input_str == "ring":
            print(ring())
        if input_str == "start":
            print(start())
        if input_str == "end":
            print(end())
        if input_str == "stabilize":
            print(stabilize())
        if input_str == "servers":
            print(servers())
        if input_str == "clients":
            print(clients())
        if input_str == "report":
            print(report())
        if input_str == "inserts":
            inserts_str = inserts()
            print("Inserts Sent: {0}\nInserts Rcvd: {1}\nAvg Hops: {2}".format(inserts_str[0],inserts_str[1],inserts_str[2]))
        if input_str == "gets":
            gets_str = gets()
            print("Gets: {0}\nAvg Hops: {1}".format(gets_str[0],gets_str[1]))
        if input_str == "keys":
            print(keys()[0])
        sys.stdout.flush()
