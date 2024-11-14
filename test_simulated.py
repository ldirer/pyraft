import bisect
import logging
import math
import os
import random
from dataclasses import dataclass
from dataclasses import field

import pytest

from raft_server import RaftServer
from raftconfig import ELECTION_TIMEOUT_TICKS_MAX
from raftconfig import HEARTBEAT_INTERVAL_TICKS
from raftconfig import NodeID
from raftlog import LogEntry
from raftlog import log_to_str
from rpc import RPC
from rpc import AddCommand
from rpc import ClockTick
from simulation import Simulation


def process_message(message, nodes_by_id):
    dest_node = nodes_by_id[message.dest]
    dest_node.process_event(message)


@pytest.mark.parametrize("iteration", range(10))
def test_log_replication(caplog, iteration):
    seed = int(os.environ.get("RAFT_SEED", str(random.randint(1, 10000))))
    random.seed(seed)
    print("random seed: ", seed)

    # we have many different clocks. One for the simulation, one for each Raft server.
    # I think this is what we want! It would let us check the behavior of misbehaving server clocks for instance,
    # possibly surfacing timing issues.
    # the dichotomy between "ticks" and "real time measured in seconds" is a bit annoying at the moment.
    sim = Simulation()

    config = {
        1: ("localhost", 0),
        2: ("localhost", 0),
        3: ("localhost", 0),
        4: ("localhost", 0),
        5: ("localhost", 0),
    }
    node_ids = list(config.keys())

    applied_commands = {n_id: [] for n_id in node_ids}

    class RaftTestServer(RaftServer):
        def send(self, rpc: RPC):
            delay = random.randint(1, math.floor(HEARTBEAT_INTERVAL_TICKS / 2) * 2)
            if sim.current_time > sim.all_healthy_time:
                # if the delay is made high, chances are we could lose a leader from time to time.
                # this test expects a period of "stability" at the end.
                delay = 1

            sim.after_delay(delay, lambda: process_message(message=rpc, nodes_by_id=nodes_by_id))

        def execute_command(self, command):
            applied_commands[self.node_id].append(command)

    class RaftTestServerSometimesGoingDown(RaftTestServer):
        """simulate going down by shutting down network"""

        def is_down(self):
            # disappear every 1000 ticks for 100 ticks. Ok the math is complicated...
            # 1101 // 100 = 11,  11 % 10 = 1
            # 1200 // 100 = 11,  12 % 10 = 2
            return sim.current_time < sim.all_healthy_time and (sim.current_time // 100) % 10 == self.node_id

        def send(self, rpc: RPC):
            if self.is_down():
                return
            super().send(rpc)

        # cutting 'process event' is sufficient for a node to go down. It won't try to send anything.
        def process_event(self, message):
            if self.is_down() and not isinstance(message, ClockTick):
                # print(f"{sim.current_time} - node {self.node_id} is down")
                return
            super().process_event(message)

    class RaftTestServerNotRespondingButReceiving(RaftTestServer):
        # one-way network cut: similar to the Cloudflare bug?
        def send(self, rpc: RPC): ...

    nodes = [
        RaftTestServerSometimesGoingDown(node_id=1, role="leader", configuration=config),
        RaftTestServerSometimesGoingDown(node_id=2, role="follower", configuration=config),
        RaftTestServerSometimesGoingDown(node_id=3, role="follower", configuration=config),
        RaftTestServerSometimesGoingDown(node_id=4, role="follower", configuration=config),
        RaftTestServerSometimesGoingDown(node_id=5, role="follower", configuration=config),
    ]
    assert node_ids == [n.node_id for n in nodes], "inconsistent test setup"

    nodes_by_id = {node.node_id: node for node in nodes}

    previous = Previous()
    schedule_invariant_checks(sim, nodes_by_id, previous)

    schedule_clock_ticks(sim, nodes_by_id)
    n_commands = 10
    # max_scheduled_time = 0
    max_scheduled_time = schedule_random_add_commands(
        sim, nodes_by_id, n_commands=n_commands, time_between_commands=(100, 200)
    )

    sim.all_healthy_time = max_scheduled_time + 1000
    # add a command that should come in when "all systems are stable" and we have a healthy leader.
    # this makes sure all entries in the leader log can be committed even if we just changed leader.
    sim.after_delay(
        sim.all_healthy_time + ELECTION_TIMEOUT_TICKS_MAX * 10,
        lambda: process_message(
            message=AddCommand(dest=get_current_leader(nodes_by_id).node_id, command="final command"),
            nodes_by_id=nodes_by_id,
        ),
    )

    print("START STATE: [(n.node_id, n.role, n.logic.state.current_term) for n in nodes]")
    print([(n.node_id, n.role, n.logic.state.current_term) for n in nodes])

    sim.run_until(max_scheduled_time + 10000)

    print("END STATE:   [(n.node_id, n.role, n.logic.state.current_term) for n in nodes]")
    print([(n.node_id, n.role, n.logic.state.current_term) for n in nodes])
    print("LEADERS HISTORY", previous.leaders)

    original_leader_log = nodes[0].logic.state.log
    original_leader_log_str = log_to_str(original_leader_log)
    # all we can check is that we don't have more commands than we tried to add... But we might have lost some.
    # This could be a proxy measure of availability?

    # -1 because we have that fake entry in the log.
    assert n_commands / 2 <= len(original_leader_log.log) - 1 <= n_commands + 1
    for follower in nodes[1:]:
        assert log_to_str(follower.logic.state.log) == original_leader_log_str, follower

    assert len(applied_commands[1]) == len(original_leader_log.log) - 1
    assert_state_machine_safety(nodes, applied_commands)

    assert [rec for rec in caplog.records if rec.levelno >= logging.ERROR] == []


def assert_state_machine_safety(nodes, applied_commands):
    # Check "State Machine Safety" from figure 3.
    for node in nodes:
        assert applied_commands[1] == applied_commands[node.node_id], f"failed for node {node}"


def get_current_leader(nodes_by_id) -> RaftServer | None:
    leaders = [n for n in nodes_by_id.values() if n.role == "leader"]
    if not leaders:
        return None
    return max(leaders, key=lambda n: n.logic.state.current_term)


# random thought: should be on *each node*... also this means they would not necessarily be exactly in sync.
def schedule_clock_ticks(sim, nodes_by_id):
    def get_clock_tick(node_id):
        # using this method to workaround defining the lambda in a loop (leads to scoping issues!!)
        return lambda: process_message(message=ClockTick(dest=node_id), nodes_by_id=nodes_by_id)

    for n_id in nodes_by_id.keys():
        sim.after_delay(0, get_clock_tick(n_id))

    sim.after_delay(1, lambda: schedule_clock_ticks(sim, nodes_by_id))


def schedule_random_add_commands(sim, nodes_by_id, n_commands=10, time_between_commands=(100, 200)):
    # leader might change over time. Selecting the current leader needs to happen in the relevant context.
    # this isn't perfect because a former leader might receive a command... too much subtlety to dig into this now
    def add_command_callback():
        leader = get_current_leader(nodes_by_id)
        if leader is not None:
            # give unique names to commands (might help with debugging safety issues later?)
            message = AddCommand(
                dest=leader.node_id,
                command=f"command added at {sim.current_time} (random id: {random.randint(1, 10000)})",
            )
            process_message(message=message, nodes_by_id=nodes_by_id)
        else:
            print("LOST COMMAND")

    prev_t = 0
    for _ in range(n_commands):
        t = random.randint(prev_t + time_between_commands[0], prev_t + time_between_commands[1])
        sim.after_delay(t, add_command_callback)
        prev_t = t

    return prev_t


def assert_election_safety(nodes_by_id):
    leaders = [n for n in nodes_by_id.values() if n.role == "leader"]
    assert len({leader.logic.state.current_term for leader in leaders}) == len(
        leaders
    ), "Election Safety has been broken! (Figure 3)"


@dataclass
class Previous:
    leader_id: NodeID = 1
    committed_entries: list[LogEntry] = field(default_factory=list)
    clocks: dict = field(default_factory=dict)
    simulation_clock_on_leader_change: int = 0

    # Ha! originally I was just tracking the previous leader. Turns out it does not work with this scenario (3 nodes):
    # 1. Node 1 is leader, goes offline
    # 2. Node 2 is elected leader, makes progress.
    # 3. Node 2 goes briefly offline.
    # 4. Now we have Node 3 candidate, Node 2 follower, and Node 1 still marked as leader.
    # But 1 is not a legitimate leader, and for the sake of leader completeness I don't want to compare it to node 2.
    leaders: list = field(default_factory=list)


def assert_leader_completeness(sim, nodes_by_id, previous: Previous):
    """Figure 3: "Leader Completeness"

    If a log entry is committed in a given term, then that entry will be present in the logs of the
    leaders for all higher-numbered terms.

    This is actually tricky to verify?

    We have the fresh leader. We want to check that it has all the entries committed thus far.
    I guess we just take the max of the committed index?
    Looking at just the index won't guarantee we look at the same entries though.
    previous leader could be useful.


    Bottom line: this is currently very messy, I think I should focus on collecting history during the test.
    And write assertions on that history (still asserting early to crash where the issue occurs).
    It can also be nice to have the history at the end of the test for inspection.
    """
    leader = get_current_leader(nodes_by_id)

    if previous.leaders and leader is not None:
        leader_id_previous_term = previous.leaders[-1][1]
        # only assert if the term is greater, otherwise we're looking at an old leader gone rogue
        if leader.logic.state.current_term > previous.leaders[-1][0]:
            previous_term_leader = nodes_by_id[leader_id_previous_term]
            previous_commit_index = previous_term_leader.logic.state.commit_index
            assert (
                leader.logic.state.log[: previous_commit_index + 1]
                == previous_term_leader.logic.state.log[: previous_commit_index + 1]
            )

    if leader is not None:
        previous.committed_entries = list(leader.logic.state.log[: leader.logic.state.commit_index + 1])

    if (leader is None and previous.leader_id is not None) or (
        leader is not None and leader.node_id != previous.leader_id
    ):
        print(
            f"Sim time: {sim.current_time}. Leader changed from {previous.leader_id} to {'None' if leader is None else f'{leader.node_id} (term {leader.logic.state.current_term})'} votes:",
            "no votes" if leader is None else leader.logic.votes_for_term,
        )
        previous.simulation_clock_on_leader_change = sim.current_time

        # avoid inserting duplicates, see comment on previous.leaders
        if leader is not None and leader.logic.state.current_term not in [lead[0] for lead in previous.leaders]:
            bisect.insort_right(previous.leaders, (leader.logic.state.current_term, leader.node_id))

    previous.leader_id = leader.node_id if leader is not None else None
    previous.leader_term_id = leader.node_id if leader is not None else None


def assert_clock_moving(nodes_by_id, previous: Previous):
    for node in nodes_by_id.values():
        new_clock = node.logic.clock
        assert new_clock > previous.clocks.get(node.node_id, -1), "test setup is broken"
        previous.clocks[node.node_id] = new_clock


def schedule_invariant_checks(sim, nodes_by_id, previous: Previous):
    def check_invariants():
        assert_election_safety(nodes_by_id)
        assert_leader_completeness(sim, nodes_by_id, previous)
        assert_clock_moving(nodes_by_id, previous)

    sim.after_delay(0, check_invariants)
    # I'm not exactly sure when the checks will run relative to other events...
    sim.after_delay(1, lambda: schedule_invariant_checks(sim, nodes_by_id, previous))
