import time
import sys

TERM = 1
STATE = 2
LEADER = 3
LOG = 4
COMMIT_INDEX = 5

REQUEST_VOTE = 1
VOTE = 2
HEARTBEAT = 3

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
    # LOG = [],
    COMMIT_INDEX : 0,
}

current_votes = 0

my_id = int(sys.argv[1])
n = int(sys.argv[2])

############# TIMEOUT and SYNCHRONIZATION MODULE #################

import random
from _thread import *
import threading

ELECTION_START_TIMEOUT = my_id + 1
HEARTBEAT_TIMEOUT = 0.4

last_heard_time_mutex = threading.Lock()
last_heart_beat_time_mutex = threading.Lock()
state_mutex = threading.Lock()
current_votes_mutex = threading.Lock()

LastHeardTime = 0
LastHeartBeatTime = 0

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
            send_message_to_all(HEARTBEAT)

############# TIMEOUT MODULE  END #################

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
    current_votes_mutex.release()  

    # send request vote to all
    send_message_to_all(REQUEST_VOTE)


def send_message_to_all(message_type):
    if message_type == HEARTBEAT:
        UpdateLastHeartBeatTime()

    for i in range(n):
        if i != my_id: # no need to send heartbeat to self
            write(message_type, int(i))


def parse_message(message: str):
    sentinel, sender_id, action, args = message.split(" ", 3)
    return sender_id, action, args


def handle_request_vote(message_term: int, sender_id: str):
    if message_term > get_state(TERM):
        # only vote if have not voted for this term 
        # (only higher terms, no need for lower terms b/c irrelevant)
        update_state(TERM, message_term)
        write(VOTE, sender_id, "True") # send a True vote message to sender
    
    elif message_term <= get_state(TERM):
        write(VOTE, sender_id, "False")


def handle_vote(message_term: int, decision: str):
    # if somehow state changed from candidate to follower/leader in between sending/receiving, ignore the vote
    if get_state(STATE) == C and decision == "True" and get_state(TERM) == message_term:

        current_votes_mutex.acquire()
        global current_votes
        current_votes += 1
        current_votes_local = current_votes
        current_votes_mutex.release()
    
        if current_votes_local > n/2:
            # become leader since we got majority of votes
            update_state(STATE, L) # change state to leader
            update_state(LEADER, my_id) # set leader to itself

            # let other processes know we are the leader
            send_message_to_all(HEARTBEAT)


def handle_heartbeat(message_term: int, sender_id: str):
    if get_state(STATE) != L:
        update_state(STATE, F) # make itself follower if not already set
        update_state(TERM, message_term) # update term to whatever was sent in message 
        update_state(LEADER, sender_id) # make sender the leader if not already set
        write(HEARTBEAT, sender_id)        


def reader(message: str):
    # Update the last time when we received anything
    UpdateLastHeardTime()

    # print("received something")


    sender_id, action, args = parse_message(message)
    message_term = args.split(" ")[-1]
    if message_term:
        message_term = int(message_term)

    if action == "RequestVote":
        # print("received request vote")
        handle_request_vote(message_term, sender_id)

    if action == "Vote":
        # print("received a vote")
        decision = args.split(" ")[-2] 
        handle_vote(message_term, decision)

    if action == "Heartbeat":
        # print ("received a heartbeat")
        handle_heartbeat(message_term, sender_id)


def write(request_type, receiver_id, msg = ""):
    if request_type == REQUEST_VOTE:
        print("SEND " + str(receiver_id) + " RequestVote " + str(msg) + " " + str(get_state(TERM)))
    if request_type == VOTE:
        print("SEND " + str(receiver_id) + " Vote " + str(msg) + " " + str(get_state(TERM)))
    if request_type == HEARTBEAT:
        print("SEND " + str(receiver_id) + " Heartbeat " + str(msg) + " " + str(get_state(TERM)))


def get_state(state_var):
    state_mutex.acquire()
    state_var_value = state[state_var]    
    state_mutex.release()
    return state_var_value

def update_state(state_var, new_value):
    state_mutex.acquire()
    global state
    if state[state_var] != new_value:
        if state_var == TERM:
            state[LEADER] = "null"
            print("STATE " + print_dict[LEADER] + "=null")
        state[state_var] = new_value
        print("STATE " + print_dict[state_var] + "=" + str(new_value))

    # if state_var == 4:
    #     #log  
    #     if log != new_value:
    #         print("STATE log[" + commit_index + "]=" + new_value) #not sure if index should be commit index, but I think so
    #         state["log"] = new_value

    state_mutex.release()

########## MAIN function #############

start_new_thread(timeout, ())

# print("STARTING RAFT on node " + str(my_id) + "\n")

while True:
    line = sys.stdin.readline()
    if line is not None:
        reader(line.strip())
