import time
import sys

#constants
TERM = 1
STATE = 2
LEADER = 3
LOG = 4
COMMIT_INDEX = 5

REQUEST_VOTE = 1
VOTE = 2
HEARTBEAT = 3


pid = int(sys.argv[1])
n = int(sys.argv[2])
last = None
print(f"Starting pinger {pid}", file=sys.stderr)

import random
TIMEOUT = random.randint(1, 9)

def timeout():
    pass

def parse_message(message: str):
    , sender_id, action, args = message.split(" ", 3)
    return sender_id, message, args

current_votes = 0
last_term_voted = -1

def reader(message: str):
    sender_id, message, args = parse_message(message)
    
    message_term = args.split(" ")[-1]


    if message == "RequestVote":
        print("received request vote")

        if state["state"] == "FOLLOWER":
            if message_term > last_term_voted:
                #only vote if have not voted for this term (only higher terms, no need for lower terms b/c irrelevant)
                write(VOTE, sender_id, "True") #send a True vote message to sender
                last_term_voted = message_term
            else:
                write(VOTE, sender_id, "False") #send a False vote message to sender


        else: 
            #if candidate or leader
            if message_term > state["term"]:
                #only vote if term higher than current state term
                write(VOTE, sender_id, "True") #send a True vote message to sender
                #i don't think we update state term yet
            else:
                write(VOTE, sender_id, "False") #send a False vote message to sender



    if message == "Vote":
        print("received a vote")
        decision = args.split(" ")[-2] 

        if state["state"] != "CANDIDATE":
            #if somehow state changed from candidate to follower/leader in between sending/receiving, ignore
            #Vote is only used by processes in candidate state
            return

        elif decision == "True":
            
            current_votes += 1
            if current_votes > n/2:
                #become leader if majority of votes
                
                # or just use the heartbeat thread 
                for i in range(n):
                    if i != pid: # no need to send heartbeat to self
                        write(HEARTBEAT, int(i))



                update_state(STATE, "LEADER") #change state to leader
                update_state(LEADER, pid) #set leader to itself
        


    if message == "Heartbeat":
        print ("received a heartbeat")
        if state["state"] == "LEADER":
            # only respond if other leader has higher term, this would be caused by partitioning (unsure of specifics)
            return
        else:
            #we should only update this stuff if message_term > state["term"] i think, but this is partitioning again
            
            update_state(STATE, "FOLLOWER") #make itself follower if not already set
            update_state(LEADER, sender_id) #make sender the leader if not already set
            update_state(TERM, message_term) #update term to whatever was sent in message 
            write(HEARTBEAT, sender_id)

            #will need to add updates for log messages in CP2
        




while True:
    # print(f"SEND {(pid+1)%n} PING {pid}", flush=True)
    line = sys.stdin.readline()
    if line is not None:
        reader(line.strip())
    # print(f"Got {line.strip()}", file=sys.stderr)
    # time.sleep(2)

print(f"Pinger {pid} done", file=sys.stderr)


state = {
    "term" : 0,
    "state" = "FOLLOWER",
    "leader" = -1,
    "commit_index" = 0
}
logs = []
def write(request_type, receiver_id, msg = ""):

    if request_type == 1:
        print("SEND " + str(receiver_id) + " RequestVote " + str(msg) + " " + state[term])
    if request_type == 2:
        print("SEND " + str(receiver_id) + " Vote " + str(msg) + " " + state[term])
    if request_type == 3:
        print("SEND " + str(receiver_id) + " Heartbeat " + str(msg) + " " + state[term])


def update_state(state_var, new_value):
    if state_var == 1:
        #term
        if state["term"] != new_value:
            print("STATE term=" + new_value)
            state["term"] = new_value

    if state_var == 2:
        #state  
        if state["state"] != new_value:
            print("STATE state=" + new_value)
            state["state"] = new_value

        if new_value == "FOLLOWER":
            current_votes = 0
        
    if state_var == 3:
        #leader  
        if state["leader"] != new_value:
            print("STATE leader=" + new_value)
            state["leader"] = new_value

    if state_var == 4:
        #log  
        if log != new_value:
            print("STATE log[" + commit_index + "]=" + new_value) #not sure if index should be commit index, but I think so
            state["log"] = new_value

    if state_var == 5:
        #commit_index   
        if state["commit_index"] != new_value:
            print("STATE commit_index=" + new_value)
            state["commit_index"] = new_value    

    