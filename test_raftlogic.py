from collections import deque

from raftconfig import ELECTION_TIMEOUT_TICKS_MAX
from raftconfig import NodeID
from raftlog import LogEntry
from raftlog import log_to_str
from raftlogic import RaftLogic
from raftlogic import RaftState
from raftlogic import compute_commit_index
from rpc import RPC
from rpc import AppendEntries
from rpc import AppendEntriesResponse
from rpc import ClockTick
from test_raftlog import str_to_log


def test_log_replication_empty():
    leader = RaftLogic(node_id=1, role="leader", node_ids=[1, 2])
    follower = RaftLogic(node_id=2, role="follower", node_ids=[1, 2])

    messages = leader.update_followers()
    assert messages == [
        RPC(
            source=1,
            dest=2,
            type="AppendEntries",
            data=AppendEntries(
                term=0, leader_id=1, prev_log_term=-1, prev_log_index=0, entries=[], leader_commit_index=0
            ),
        )
    ]

    message = messages[0]
    response = follower.handle_append_entries(message)
    assert response.data == AppendEntriesResponse(term=0, success=True, match_index=0)


def test_log_replication_simple():
    leader = RaftLogic(node_id=1, role="leader", node_ids=[1, 2])
    follower = RaftLogic(node_id=2, role="follower", node_ids=[1, 2])

    leader.add_command("cmd 1")
    leader.add_command("cmd 2")

    messages = leader.update_followers()
    assert len(messages) == 1
    assert messages[0] == RPC(
        source=1,
        dest=2,
        type="AppendEntries",
        data=AppendEntries(
            term=0,
            leader_id=1,
            prev_log_term=-1,
            prev_log_index=0,
            entries=[LogEntry(command="cmd 1", term=0), LogEntry(command="cmd 2", term=0)],
            leader_commit_index=0,
        ),
    )

    message = messages[0]
    response = follower.handle_append_entries(message)
    assert response.data == AppendEntriesResponse(term=0, success=True, match_index=2)

    assert log_to_str(follower.state.log) == "00"


def propagate(messages, nodes_by_id) -> int:
    """fake network, send messages between nodes (including messages in response to message handling)"""
    messages = deque(messages)
    counter = 0
    while messages:
        msg = messages.popleft()
        dest_node = nodes_by_id[msg.dest]
        responses = dest_node.handle_message(msg)
        messages.extend(responses)

        counter += 1
        if counter > 10000:
            raise ValueError("too many messages exchanged, interrupting")
    return counter


def create_cluster_figure_7() -> dict[NodeID, RaftLogic]:
    z = "1114455666"
    leader_log = str_to_log(z)
    a = str_to_log("111445566")
    b = str_to_log("1114")
    c = str_to_log("11144556666")
    d = str_to_log("111445566677")
    e = str_to_log("1114444")
    f = str_to_log("11122233333")

    node_ids = list(range(1, 8))

    leader = RaftLogic(node_id=1, role="leader", node_ids=node_ids, state=RaftState(current_term=8, log=leader_log))
    leader.become_leader()
    followers = [
        RaftLogic(node_id=node_id, role="follower", node_ids=node_ids, state=state)
        for node_id, state in enumerate(
            [
                # we need to make assumptions on terms
                # but not every combination is valid: we had a leader for term 8 so at least 4 nodes have seen term 8.
                RaftState(log=a, current_term=8),
                RaftState(log=b, current_term=8),
                RaftState(log=c, current_term=8),
                RaftState(log=d, current_term=8),
                RaftState(log=e, current_term=8),
                RaftState(log=f, current_term=8),
            ],
            start=2,
        )
    ]

    nodes = followers + [leader]
    nodes_by_id = {node.node_id: node for node in nodes}
    return nodes_by_id


def test_log_replication_figure_7():
    nodes_by_id = create_cluster_figure_7()
    leader = nodes_by_id[1]
    initial_leader_log = log_to_str(leader.state.log)

    messages = leader.update_followers()
    message_count = propagate(messages, nodes_by_id)

    print(f"Messages exchanged for initial sync: {message_count}")

    # all logs should be in agreement - not necessarily exactly equal. Extra items might remain in followers.
    assert log_to_str(nodes_by_id[2].state.log) == initial_leader_log
    assert log_to_str(nodes_by_id[3].state.log) == initial_leader_log
    assert log_to_str(nodes_by_id[4].state.log) == "11144556666"
    assert log_to_str(nodes_by_id[5].state.log) == "111445566677"
    assert log_to_str(nodes_by_id[6].state.log) == initial_leader_log
    assert log_to_str(nodes_by_id[7].state.log) == initial_leader_log

    messages = leader.add_command("extra command in term 8")
    message_count = propagate(messages, nodes_by_id)
    print(f"Messages exchanged for extra command: {message_count}")

    new_leader_log = log_to_str(leader.state.log)
    assert new_leader_log == "11144556668"

    assert log_to_str(nodes_by_id[2].state.log) == new_leader_log
    assert log_to_str(nodes_by_id[3].state.log) == new_leader_log
    assert log_to_str(nodes_by_id[4].state.log) == new_leader_log
    assert log_to_str(nodes_by_id[5].state.log) == new_leader_log
    assert log_to_str(nodes_by_id[6].state.log) == new_leader_log
    assert log_to_str(nodes_by_id[7].state.log) == new_leader_log

    assert leader.state.next_index == {i: len(new_leader_log) + 1 for i in range(2, 8)}
    assert leader.state.match_index == {i: len(new_leader_log) for i in range(2, 8)}

    # check that leader has the correct idea of followers state
    rpcs = leader.update_followers()
    assert all(len(rpc.data.entries) == 0 for rpc in rpcs)


def test_compute_commit_index():
    assert (
        compute_commit_index(
            {
                1: 8,
                2: 7,
                # number 3 is the leader
                4: 6,
                5: 5,
            }
        )
        == 7
    )

    # weird cluster with 4 machines requires 3 machines for a quorum.
    assert (
        compute_commit_index(
            {
                1: 8,
                2: 7,
                4: 6,
            }
        )
        == 7
    )


def test_election_figure_7():
    nodes_by_id = create_cluster_figure_7()

    assert nodes_by_id[2].role == "follower", "wrong test setup"

    nodes_by_id[2].clock += ELECTION_TIMEOUT_TICKS_MAX
    messages = [ClockTick(dest=2)]
    message_count = propagate(messages, nodes_by_id)
    print("election happened with message_count:", message_count)

    # node a
    assert nodes_by_id[2].state.current_term == 9
    assert nodes_by_id[2].role == "leader"
    assert nodes_by_id[2].votes_for_term == {1: False, 2: True, 3: True, 4: False, 5: False, 6: True, 7: True}

    # former leader should have stepped down
    assert nodes_by_id[1].role == "follower"

    # node b
    # start from scratch because after the election all logs were synced.
    # Not as interesting a test, everyone can become leader.
    nodes_by_id = create_cluster_figure_7()

    nodes_by_id[3].clock += ELECTION_TIMEOUT_TICKS_MAX
    messages = [ClockTick(dest=3)]
    _ = propagate(messages, nodes_by_id)

    assert nodes_by_id[3].state.current_term == 9
    assert nodes_by_id[3].votes_for_term == {1: False, 2: False, 3: True, 4: False, 5: False, 6: False, 7: True}
    assert nodes_by_id[3].role == "candidate"

    # node c
    nodes_by_id = create_cluster_figure_7()
    nodes_by_id[4].clock += ELECTION_TIMEOUT_TICKS_MAX
    messages = [ClockTick(dest=4)]
    _ = propagate(messages, nodes_by_id)

    assert nodes_by_id[4].state.current_term == 9
    assert nodes_by_id[4].role == "leader"
    assert nodes_by_id[4].votes_for_term == {1: True, 2: True, 3: True, 4: True, 5: False, 6: True, 7: True}

    # node d
    nodes_by_id = create_cluster_figure_7()
    nodes_by_id[5].clock += ELECTION_TIMEOUT_TICKS_MAX
    messages = [ClockTick(dest=5)]
    _ = propagate(messages, nodes_by_id)

    assert nodes_by_id[5].state.current_term == 9
    assert nodes_by_id[5].role == "leader"
    assert nodes_by_id[5].votes_for_term == {1: True, 2: True, 3: True, 4: True, 5: True, 6: True, 7: True}
