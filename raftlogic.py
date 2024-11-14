import logging
import random
from dataclasses import dataclass
from dataclasses import field

from raftconfig import ELECTION_TIMEOUT_TICKS_MAX
from raftconfig import ELECTION_TIMEOUT_TICKS_MIN
from raftconfig import HEARTBEAT_INTERVAL_TICKS
from raftconfig import NodeID
from raftconfig import NodeRole
from raftlog import RaftLog
from raftlog import log_to_str
from rpc import RPC
from rpc import AddCommand
from rpc import AppendEntries
from rpc import AppendEntriesResponse
from rpc import ClockTick
from rpc import RaftMessage
from rpc import RequestVote
from rpc import RequestVoteResponse

logger = logging.getLogger(__name__)


@dataclass
class RaftState:
    # persisted, all servers
    log: RaftLog = field(default_factory=RaftLog)
    current_term: int = 0
    voted_for: NodeID | None = None

    # volatile, all servers
    commit_index: int = 0
    last_applied: int = 0

    # volatile, on leaders only
    # for each server, index of the next log entry to send to that server
    next_index: dict = field(default_factory=dict)
    # for each server, index of highest log entry known to be replicated on that server
    match_index: dict = field(default_factory=dict)


class RaftLogic:
    """forbidden: threads, networking."""

    def __init__(self, node_id: NodeID, role: NodeRole, node_ids: list[NodeID], state: RaftState | None = None):
        self.node_ids = node_ids
        self.node_id = node_id
        self.role = role
        self.state = state if state is not None else RaftState()
        self.clock = 0
        self.time_last_heartbeat = -(HEARTBEAT_INTERVAL_TICKS + 1)
        self.time_last_heard_of_potential_leader = 0

        self.random = random.Random(self.node_id)

        self.election_due = 0
        self.reset_election_timer()

        # not on "RaftState" because I now feel like state should be inlined in this class
        self.votes_for_term = None if role != "candidate" else {n_id: False for n_id in self.node_ids}

        if self.role == "leader":
            self.become_leader()

    def __repr__(self):
        return f"RaftLogic(node_id={self.node_id}, role='{self.role}', log={log_to_str(self.state.log)})"

    def add_command(self, command) -> list[RPC]:
        assert self.role == "leader"
        self.state.log.append_new_command_as_leader(leader_term=self.state.current_term, command=command)
        return self.update_followers()

    def handle_message(self, message: RaftMessage) -> list[RPC]:
        prev_term = self.state.current_term
        prev_commit_index = self.state.commit_index
        prev_log = list(self.state.log.log)

        outgoing = self._handle_message(message)

        assert self.state.current_term >= prev_term
        assert self.state.commit_index >= prev_commit_index
        assert self.state.commit_index <= self.state.log.last_log_index()

        if self.role == "leader":
            assert self.state.log.log[: len(prev_log)] == prev_log, "Leader Append-Only property violated (figure 3)"

        return outgoing

    def _handle_message(self, message: RaftMessage) -> list[RPC]:
        if isinstance(message, AddCommand):
            return self.add_command(message.command)
        elif isinstance(message, ClockTick):
            return self.handle_clock_tick()
        elif isinstance(message, RPC):
            response = self.handle_rpc(message)
            if isinstance(response, list):
                return response
            return [response] if response is not None else []
        else:
            raise ValueError("unknown message type: %s", message)

    def handle_rpc(self, message) -> RPC | list[RPC] | None:
        self.rule_check_rpc_term(message.data)
        if self.state.current_term > message.data.term:
            # technically we could respond so that the other machine can update its term.
            # just never a success response. For now ignoring will do.
            logger.info("ignoring stale RPC %s", message)
            return

        match message.type:
            case "AppendEntriesResponse":
                return self.handle_append_entries_response(message)
            case "AppendEntries":
                return self.handle_append_entries(message)
            case "RequestVote":
                return self.handle_request_vote(message)
            case "RequestVoteResponse":
                return self.handle_request_vote_response(message)
            case _:
                raise ValueError("unknown rpc type")

    def handle_append_entries(self, rpc: RPC) -> RPC:
        success = self._handle_append_entries(rpc.data)

        match_index = rpc.data.prev_log_index + len(rpc.data.entries)
        # only update commit index on success: we want to make sure that everything before our commit index is correct.
        if success and rpc.data.leader_commit_index > self.state.commit_index:
            # as in figure 2. I think the `min` is meant to accommodate AppendEntries calls that don't include
            # all the entries the leader has at the time of sending.
            self.state.commit_index = min(rpc.data.leader_commit_index, rpc.data.prev_log_index + len(rpc.data.entries))

        response_rpc_data = AppendEntriesResponse(
            term=self.state.current_term, success=success, match_index=match_index
        )
        return RPC(source=self.node_id, type="AppendEntriesResponse", dest=rpc.source, data=response_rpc_data)

    def _handle_append_entries(self, msg: AppendEntries) -> bool:
        if self.role == "leader":
            # ignore this request, (we already checked term was not higher than self).
            logger.info("ignoring AppendEntries request since node is leader.")
            return False

        if self.role == "candidate" and self.state.current_term <= msg.term:
            self.convert_to("follower")

        if msg.term < self.state.current_term:
            return False

        # legitimate leader request
        self.reset_election_timer()

        return self.state.log.append_entries(
            prev_index=msg.prev_log_index, prev_term=msg.prev_log_term, entries=msg.entries
        )

    def handle_append_entries_response(self, rpc: RPC) -> RPC | None:
        msg: AppendEntriesResponse = rpc.data
        source = rpc.source
        if self.role != "leader":
            # don't know what to return exactly but in this case we don't want to do anything.
            # not leader anymore = not this node's problem
            return

        if msg.success:
            # idea: without 'max', out-of-order messages should lead to weird bugs. See if a simulation can find them.
            # Tested, looks like the simulation triggered it (commit index not monotonically increasing).
            self.state.match_index[source] = max(self.state.match_index[source], msg.match_index)
            self.state.next_index[source] = self.state.match_index[source] + 1

            new_commit_index = compute_commit_index(self.state.match_index)
            # see section 5.4.2 in the paper, an entry replicated on a majority of servers can't be
            # committed unless an entry from this leader's term has been committed.
            if self.state.log[new_commit_index].term == self.state.current_term:
                self.state.commit_index = new_commit_index
            return

        if not msg.success:
            # We could get an old response here? in that case we probably don't want to decrement?
            # say we try 3 times to send AppendEntries.
            # then we get all 3 responses at the same time. We don't want to do -3.
            # because of that we do need to make sure we don't go below 1 though.
            # next_index is meant to optimize calls, the system should self-correct if it's wrong.
            self.state.next_index[source] = max(self.state.next_index[source] - 1, 1)
            return self.update_follower(follower_id=source)

    def rule_check_rpc_term(self, rpc):
        # figure 2 'Rules for servers':
        # "If RPC request or response contains term T > currentTerm, set currentTerm = T, convert to follower"
        if rpc.term > self.state.current_term:
            self.state.current_term = rpc.term
            self.state.voted_for = None
            self.convert_to(new_role="follower")

    def convert_to(self, new_role: NodeRole):
        assert_transition_allowed(self.role, new_role)
        logger.info("Node %s converting to %s", self, new_role)
        # note we might convert from candidate to candidate: this starts a new election
        if new_role == "candidate":
            self.state.current_term += 1
            self.votes_for_term = {n_id: False for n_id in self.node_ids}
            self.votes_for_term[self.node_id] = True
            self.state.voted_for = self.node_id
            self.reset_election_timer()
        else:
            # Not resetting because it's nice to check that in tests.
            # self.votes_for_term = None
            pass

        if new_role == "leader":
            self.become_leader()

        if new_role == "follower":
            self.reset_election_timer()

        self.role = new_role

    def become_leader(self):
        self.state.next_index = {
            n_id: self.state.log.last_log_index() + 1 for n_id in self.node_ids if n_id != self.node_id
        }
        self.state.match_index = {n_id: 0 for n_id in self.node_ids if n_id != self.node_id}

    def update_followers(self) -> list[RPC]:
        rpcs = [self.update_follower(follower_id) for follower_id in self.node_ids if follower_id != self.node_id]
        self.time_last_heartbeat = self.clock
        return [rpc for rpc in rpcs if rpc is not None]

    def update_follower(self, follower_id: NodeID) -> RPC | None:
        next_index = self.next_index_for_node(follower_id)
        return RPC(
            source=self.node_id,
            dest=follower_id,
            type="AppendEntries",
            data=AppendEntries(
                term=self.state.current_term,
                leader_id=self.node_id,
                prev_log_index=next_index - 1,
                prev_log_term=self.state.log[next_index - 1].term,
                entries=self.state.log[next_index:],
                leader_commit_index=self.state.commit_index,
            ),
        )

    def next_index_for_node(self, node) -> int:
        return self.state.next_index[node]

    def handle_request_vote(self, rpc) -> RPC:
        msg: RequestVote = rpc.data
        # idea: test network difficulties during election causing multiple voting requests from one candidate
        # figure 2 'RequestVote RPC'
        vote_granted = (
            self.state.current_term <= msg.term
            and (self.state.voted_for is None or self.state.voted_for == msg.candidate_id)
            and not self.state.log.is_more_up_to_date(
                other_last_index=msg.last_log_index, other_last_term=msg.last_log_term
            )
        )

        if vote_granted:
            self.state.voted_for = msg.candidate_id
            self.reset_election_timer()

        return RPC(
            source=self.node_id,
            dest=rpc.source,
            type="RequestVoteResponse",
            data=RequestVoteResponse(term=self.state.current_term, vote_granted=vote_granted),
        )

    def handle_request_vote_response(self, rpc):
        msg: RequestVoteResponse = rpc.data

        # need to double-check the term is correct to avoid using an old, delayed message
        if msg.term == self.state.current_term:
            self.votes_for_term[rpc.source] = msg.vote_granted

        if self.role != "candidate":
            # nothing to do: we're already leader or we reverted back to follower
            # BUT for testing convenience we collect all our votes. Feels good too.
            return

        if sum(self.votes_for_term.values()) >= (len(self.node_ids) + 1) // 2:
            # I deserve this promotion.
            self.convert_to("leader")
            # immediately send heartbeats. We could also have set things up so that this happens on next tick. Whatever.
            return self.update_followers()

    def handle_clock_tick(self) -> list[RPC]:
        self.clock += 1
        if self.role == "leader" and (self.clock - self.time_last_heartbeat) >= HEARTBEAT_INTERVAL_TICKS:
            return self.update_followers()

        if self.role != "leader" and self.clock >= self.election_due:
            # start a new election \o/
            self.convert_to("candidate")
            return self.request_votes()

        return []

    def request_votes(self) -> list[RPC]:
        return [self._request_vote(follower_id) for follower_id in self.node_ids if follower_id != self.node_id]

    def _request_vote(self, node_id: NodeID) -> RPC:
        last_log_index = self.state.log.last_log_index()
        return RPC(
            source=self.node_id,
            dest=node_id,
            type="RequestVote",
            data=RequestVote(
                term=self.state.current_term,
                candidate_id=self.node_id,
                last_log_index=last_log_index,
                last_log_term=self.state.log[last_log_index].term,
            ),
        )

    def reset_election_timer(self):
        self.election_due = self.clock + self.random.randrange(ELECTION_TIMEOUT_TICKS_MIN, ELECTION_TIMEOUT_TICKS_MAX)


def assert_transition_allowed(type: NodeRole, new_type: NodeRole):
    if (type, new_type) not in {
        ("follower", "follower"),
        ("follower", "candidate"),
        ("candidate", "candidate"),
        ("candidate", "leader"),
        ("candidate", "follower"),
        ("leader", "follower"),
    }:
        logger.error("forbidden transition from %s to %s detected!", type, new_type)


def compute_commit_index(match_index: dict):
    return sorted(match_index.values())[len(match_index) // 2]
