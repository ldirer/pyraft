# Mapping of logical server numbers to network addresses.  All network
# operations in Raft will use the logical server numbers (e.g., send
# a message from node 2 to node 4).
import math
from typing import Literal

SERVERS = {
    1: ("localhost", 11000),
    2: ("localhost", 12000),
    3: ("localhost", 13000),
    4: ("localhost", 14000),
    5: ("localhost", 15000),
}

NodeID = int
NodeRole = Literal["leader", "candidate", "follower"]
CLOCK_TICK_INTERVAL_S = 0.01
HEARTBEAT_INTERVAL_S = 0.1
# the paper mentions 'election timeouts are chosen randomly from a fixed interval (e.g. 150-300ms)
ELECTION_TIMEOUT_MIN_S = 0.150
ELECTION_TIMEOUT_MAX_S = 0.300

# make logs easier to look at when debugging
# HEARTBEAT_INTERVAL_S *= 10
# ELECTION_TIMEOUT_MIN_S *= 10
# ELECTION_TIMEOUT_MAX_S *= 10

HEARTBEAT_INTERVAL_TICKS = HEARTBEAT_INTERVAL_S / CLOCK_TICK_INTERVAL_S
ELECTION_TIMEOUT_TICKS_MIN: int = math.floor(ELECTION_TIMEOUT_MIN_S / CLOCK_TICK_INTERVAL_S)
ELECTION_TIMEOUT_TICKS_MAX: int = math.floor(ELECTION_TIMEOUT_MAX_S / CLOCK_TICK_INTERVAL_S)
