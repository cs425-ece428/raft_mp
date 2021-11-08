import time
import sys

pid = int(sys.argv[1])
n = int(sys.argv[2])
last = None
print(f"Starting pinger {pid}", file=sys.stderr)


def reader():


while True:
    print(f"SEND {(pid+1)%n} PING {pid}", flush=True)
    line = sys.stdin.readline()
    if line is None:
        break
    print(f"Got {line.strip()}", file=sys.stderr)
    time.sleep(2)

print(f"Pinger {pid} done", file=sys.stderr)


state = {
    "term" : 0,
    "state" = "FOLLOWER",
    "leader" = -1,
    "commit_index" = 0
}
logs = []
def write(request_type, receiver_id, msg):

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

    