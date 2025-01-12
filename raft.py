import time
import sys

############### STATE VARIABLES ###################

TERM = 1
STATE = 2
LEADER = 3
LOG = 4
COMMIT_INDEX = 5

REQUEST_VOTE = 1
VOTE = 2
APPEND_ENTRIES = 3
APPEND_ENTRIES_RESPONSE = 4
HEARTBEAT = 5

print_dict = {
    TERM: "term",
    STATE: "state",
    LEADER: "leader",
    LOG: "log",
    COMMIT_INDEX: "commitIndex"
}

L = "\"LEADER\""
C = "\"CANDIDATE\""
F = "\"FOLLOWER\""

state = {
    TERM : 0,
    STATE : F,
    LEADER : "null",
    LOG : [(0, "")],
    COMMIT_INDEX : 0,
}
peer_state = []
my_match_index = 0

current_votes = 0

my_id = int(sys.argv[1])
n = int(sys.argv[2])

############# TIMEOUT and SYNCHRONIZATION MODULE #################

import random
from _thread import *
import threading

ELECTION_START_TIMEOUT = (0.2 + my_id/5 + random.random())
HEARTBEAT_TIMEOUT = 0.1

last_heard_time_mutex = threading.Lock()
last_heart_beat_time_mutex = threading.Lock()
state_mutex = threading.Lock()
current_votes_mutex = threading.Lock()

LastHeardTime = time.time()
LastHeartBeatTime = time.time()

def GetLastHeardTime():
    last_heard_time_mutex.acquire()
    last_heard_time = LastHeardTime
    last_heard_time_mutex.release()
    return last_heard_time

def UpdateLastHeardTime():
    last_heard_time_mutex.acquire()
    global LastHeardTime
    LastHeardTime = time.time()
    last_heard_time_mutex.release()

def GetLastHeartBeatTime():
    last_heart_beat_time_mutex.acquire()
    last_heart_beat_time = LastHeartBeatTime
    last_heart_beat_time_mutex.release()
    return last_heart_beat_time

def UpdateLastHeartBeatTime():
    last_heart_beat_time_mutex.acquire()
    global LastHeartBeatTime
    LastHeartBeatTime = time.time()
    last_heart_beat_time_mutex.release()

def timeout():
    while True:
        if time.time() - GetLastHeardTime() > ELECTION_START_TIMEOUT and get_state(STATE) != L:
            start_election()
            UpdateLastHeardTime()
            
        elif time.time() - GetLastHeartBeatTime() > HEARTBEAT_TIMEOUT and get_state(STATE) == L:
            # send_message_to_all(HEARTBEAT)
            send_message_to_all(APPEND_ENTRIES)
            # UpdateLastHeartBeatTime()

############# STARTING AN ELECTION #################

def start_election():
    # increment current term
    current_term = get_state(TERM)
    update_state(TERM, current_term + 1)

    # update state to candidate
    update_state(STATE, C)

    # vote for yourself
    current_votes_mutex.acquire()
    global current_votes
    current_votes = 1
    current_votes_local = current_votes
    current_votes_mutex.release()  

    if current_votes_local > n/2:
        # become leader since we got majority of votes
        declare_leader()
        return

    # send request vote to all
    send_message_to_all(REQUEST_VOTE)


################### HELPER FUNCTIONS ########################


def send_message_to_all(message_type, msg = ""):
    if message_type == APPEND_ENTRIES:
        UpdateLastHeartBeatTime()

    for i in range(n):
        if i != my_id: # no need to send heartbeat to self
            write(message_type, int(i), msg)


def parse_message(message: str):
    sentinel, sender_id, action, args = message.split(" ", 3)
    return sender_id, action, args


def declare_leader():

        global peer_state

        update_state(STATE, L) # change state to leader
        update_state(LEADER, my_id) # set leader to itself

        num_log_entries = len(get_state(LOG))
        for i in range(0, n):
            peer_state.append({
                "next_index": num_log_entries,
                "match_index": 0
            })
                
        # let other processes know we are the leader
        # UpdateLastHeartBeatTime()
        send_message_to_all(APPEND_ENTRIES)


def log_contains_entry(log_index: int, log_term: int) -> bool:
    local_log = get_state(LOG)
    if len(local_log) > log_index and local_log[log_index][0] == log_term:
        return True
    return False


def check_and_commit():
    logs = get_state(LOG)
    commit_index = get_state(COMMIT_INDEX)
    current_term = get_state(TERM)

    next_index = len(logs)

    for index in range (commit_index + 1, next_index):
        commit_votes = 1

        # Can only commit messages logged in our term
        if logs[index][0] == current_term:
            for i in range (0, n):
                if i != my_id:
                    peer_match_index = peer_state[i]["match_index"]
                    if peer_match_index >= index:
                        commit_votes += 1

            if commit_votes > n/2:
                update_state(COMMIT_INDEX, index)
                # send_message_to_all(APPEND_ENTRIES)


################# HANDLERS FOR PROCESSING RECEIVED MESSAGES ######################


def handle_request_vote(
    sender_id: int, 
    message_term: int, 
    candidate_last_log_index: int, 
    candidate_last_log_term: int
    ):
    if message_term > get_state(TERM):
        # only vote if have not voted for this term 
        # (only higher terms, no need for lower terms b/c irrelevant)
        update_state(STATE, F)
        update_state(TERM, message_term)
    
        local_log = get_state(LOG)
        my_last_log_term = local_log[-1][0]
        my_last_log_index = len(local_log)

        # Send a vote only if the candidate's logs are more complete
        if my_last_log_term <= candidate_last_log_term or (my_last_log_term == candidate_last_log_term and my_last_log_index <= candidate_last_log_index):
            write(VOTE, sender_id, "True") 
        else:
            write(VOTE, sender_id, "False")
    
    elif message_term <= get_state(TERM):
        write(VOTE, sender_id, "False")


def handle_vote(
    message_term: int, 
    decision: str
    ):
    # if somehow state changed from candidate to follower/leader in between sending/receiving, ignore the vote
    if get_state(STATE) == C and decision == "True" and get_state(TERM) == message_term:

        current_votes_mutex.acquire()
        global current_votes
        current_votes += 1
        current_votes_local = current_votes
        current_votes_mutex.release()
    
        if current_votes_local > n/2:
            # become leader since we got majority of votes
            declare_leader()


def handle_appendentries(
    sender_id: int, 
    current_term: int, 
    prev_log_index: int, 
    prev_log_term: int, 
    commit_index: int,
    log_message: str,
    log_message_term: int
    ):
    global my_match_index
    if (get_state(STATE) != L and current_term == get_state(TERM)) or current_term > get_state(TERM):
        update_state(STATE, F) # make itself follower if not already set
        update_state(TERM, current_term) # update term to whatever was sent in message 
        update_state(LEADER, sender_id) # make sender the leader if not already set

        success = log_contains_entry(prev_log_index, prev_log_term)

        if success:
            my_match_index = max(my_match_index, prev_log_index)

            if log_message:
                # add/overwrite the entry to our logs
                update_state(LOG, log_message, log_message_term, prev_log_index + 1)
                # increment the match index
                my_match_index = max(my_match_index, prev_log_index + 1)
            
            # commit the new log if our commit index < leader's commit index
            if commit_index > get_state(COMMIT_INDEX):
                update_state(COMMIT_INDEX, min(commit_index, my_match_index))
        
        write(APPEND_ENTRIES_RESPONSE, sender_id, "1" if success else "0")


def handle_appendentries_response(
    sender_id: int, 
    message_term: int, 
    success: bool, 
    match_index: int
    ):
    global peer_state

    if get_state(STATE) == L:
        if success:
            # If match_index is less than our next index, send an appendentry with the next log
            peer_state[sender_id]["match_index"] = match_index
            peer_state[sender_id]["next_index"] = match_index + 1
            # if match_index < len(get_state(LOG))-1:
            #     write(APPEND_ENTRIES, sender_id)

            # Check if we can commit anything. If yes, commit and send appendentries to all
            check_and_commit()
        else:
            peer_state[sender_id]["next_index"] -= 1
            # write(APPEND_ENTRIES, sender_id)


def handle_log(log_message : str):
    global peer_state

    # only the leader should get new logs
    if get_state(STATE) == L:
        # update our logs
        num_log_entries = len(get_state(LOG))
        update_state(LOG, log_message, get_state(TERM), num_log_entries)
        # update our match index and next index
        peer_state[my_id]["match_index"] = num_log_entries - 1
        peer_state[my_id]["next_index"] = num_log_entries
        # broadcast the new log entry
        # send_message_to_all(APPEND_ENTRIES)


####################### I/O WITH OUTSIDE WORLD ##########################


def reader(message: str):
    # Update the last time when we received anything
    UpdateLastHeardTime()

    if message.split(' ')[0] == 'LOG':
        log_message = message.split(' ')[1]
        handle_log(log_message)
        return

    sender_id, action, args = parse_message(message)

    sender_id = int(sender_id)
    args_split = args.split(" ")

    if action == "RequestVote":
        # print("received request vote")
        message_term, candidate_last_log_index, candidate_last_log_term = map(int, args_split)
        handle_request_vote(sender_id, message_term, candidate_last_log_index, candidate_last_log_term)

    if action == "Vote":
        # print("received a vote")
        message_term = int(args_split[0])
        decision = args_split[1] 
        handle_vote(message_term, decision)

    if action == "AppendEntries":
        # print ("received a appendentries")
        if len(args_split) == 6:
            message_term, prev_log_index, prev_log_term, commit_index = map(int, args_split[:-2])
            log_message, log_message_term = args_split[-2:]
            log_message_term = int(log_message_term)
        else:
            message_term, prev_log_index, prev_log_term, commit_index = map(int, args_split)
            log_message = ""
            log_message_term = -1

        handle_appendentries(sender_id, message_term, prev_log_index, prev_log_term, commit_index, log_message, log_message_term)

    if action == "AppendEntriesResponse":
        # print ("received a appendentries response")
        message_term = int(args_split[0])
        success = bool(int(args_split[1])) # "0" -> 0 -> False
        match_index = int(args_split[2])

        handle_appendentries_response(sender_id, message_term, success, match_index)


def write(request_type, receiver_id, msg = ""):
    logs = get_state(LOG)
    term = str(get_state(TERM))
    
    if request_type == REQUEST_VOTE:
        last_log_index = len(logs) - 1
        last_log_term = logs[-1][0]
        print("SEND " + str(receiver_id) + " RequestVote " 
            + term + " " 
            + str(last_log_index) + " " 
            + str(last_log_term)
        )
    if request_type == VOTE:
        print("SEND " + str(receiver_id) + " Vote " + term + " " + str(msg))
    if request_type == APPEND_ENTRIES:
        prev_log_index = max(peer_state[receiver_id]["next_index"] - 1, 0)
        prev_log_term = logs[prev_log_index][0]
        log_message = logs[prev_log_index + 1][1] if prev_log_index + 1 < len(logs) else ""
        log_message_term = logs[prev_log_index + 1][0] if prev_log_index + 1 < len(logs) else ""
        print("SEND " + str(receiver_id) + " AppendEntries " 
            + term + " " 
            + str(prev_log_index) + " "
            + str(prev_log_term) + " "
            + str(get_state(COMMIT_INDEX)) + " "
            + log_message + " "
            + str(log_message_term)
        )
    if request_type == APPEND_ENTRIES_RESPONSE:
        print("SEND " + str(receiver_id) + " AppendEntriesResponse " + 
            term + " " + 
            str(msg) + " " + 
            str(my_match_index)
        )

    if request_type == HEARTBEAT:
        print("SEND " + str(receiver_id) + " HeartBeat " + 
            term 
        )

################ STATE ACCESS AND MANIPULATION #################

def get_state(state_var):
    state_mutex.acquire()
    state_var_value = state[state_var]    
    state_mutex.release()
    return state_var_value

def update_state(state_var, new_value, log_message_term = -1, log_message_index = -1):
    state_mutex.acquire()
    global state
    global my_match_index

    if state_var == TERM:
        if state[state_var] != new_value:
            state[LEADER] = "null"
            print("STATE " + print_dict[LEADER] + "=null")
            state[state_var] = new_value
            print("STATE " + print_dict[state_var] + "=" + str(new_value))
            my_match_index = 0


    elif state_var == STATE:
        if state[state_var] != new_value:
            state[state_var] = new_value
            print("STATE " + print_dict[state_var] + "=" + str(new_value))

    elif state_var == LEADER:
        if state[state_var] != new_value:
            state[state_var] = new_value
            print("STATE " + print_dict[state_var] + "=" + str(new_value))
    

    elif state_var == LOG:
        log = state[LOG]

        new_entry = [int(log_message_term), new_value]
        
        if len(log) == log_message_index:
            log.append(new_entry)
        elif len(log) > log_message_index:
            log[log_message_index] = new_entry
        else:
            print(len(log))
            print(log_message_index)
            raise Exception("bad log message index")

        print("STATE log[" + str(log_message_index) + "]=[" + str(log_message_term) + ", \"" + new_value + "\"]") 
        state[LOG] = log

        # # appending value to follower
        # elif len(log) == my_match_index:
        #     print("STATE log[" + str(len(log)) + "]=[" + str(new_value[0]) + ", \"" + new_value[1] + "\"]") 
        #     log.append(new_value)

        # # overwriting value to follower
        # elif log[my_match_index] != new_value:
        #     print("STATE log[" + str(my_match_index) + "]=[" + str(new_value[0]) + ", \"" + new_value[1] + "\"]")  
        #     log[my_match_index] = new_value

    elif state_var == COMMIT_INDEX:
        if state[state_var] != new_value:
            # If leader, need to print COMMITED
            if state[STATE] == L:
                for i in range(state[state_var]+1, new_value+1):
                    # print "COMMITTED <string> k"
                    print("COMMITED " + state[LOG][i][1] + " " + str(i))

            state[state_var] = new_value
            print("STATE " + print_dict[state_var] + "=" + str(new_value))      

    state_mutex.release()

########## MAIN function #############

start_new_thread(timeout, ())

# print("STARTING RAFT on node " + str(my_id) + "\n")

while True:
    line = sys.stdin.readline()
    if line is not None:
        reader(line.strip())
