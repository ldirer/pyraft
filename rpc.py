import json
from dataclasses import dataclass
from typing import Literal
from typing import Union

from raftconfig import NodeID
from raftlog import LogEntry


class RaftMessage:
    pass


@dataclass
class RPC(RaftMessage):
    source: NodeID
    dest: NodeID
    type: Literal["AppendEntries", "AppendEntriesResponse", "RequestVoteResponse", "RequestVote"]
    data: Union["AppendEntries", "AppendEntriesResponse", "RequestVoteResponse", "RequestVote"]

    def as_dict(self):
        return {
            "source": self.source,
            "dest": self.dest,
            "type": self.type,
            "data": self.data.as_dict(),
        }

    def serialize(self) -> bytes:
        return json.dumps(self.as_dict()).encode("utf-8")

    @classmethod
    def deserialize(cls, msg: bytes) -> "RPC":
        msg_data = json.loads(msg.decode("utf-8"))

        match msg_data["type"]:
            case "AppendEntries":
                parsed_data = AppendEntries.from_dict(msg_data["data"])
            case "AppendEntriesResponse":
                parsed_data = AppendEntriesResponse(**msg_data["data"])
            case "RequestVote":
                parsed_data = RequestVote(**msg_data["data"])
            case "RequestVoteResponse":
                parsed_data = RequestVoteResponse(**msg_data["data"])
            case _:
                raise ValueError("unknown RPC type, unable to deserialize")

        return RPC(
            source=msg_data["source"], dest=msg_data["dest"], type=parsed_data.__class__.__name__, data=parsed_data
        )


@dataclass
class AppendEntries:
    term: int
    leader_id: NodeID
    prev_log_index: int
    prev_log_term: int
    entries: list[LogEntry]
    leader_commit_index: int

    def as_dict(self):
        return {
            "term": self.term,
            "leader_id": self.leader_id,
            "prev_log_index": self.prev_log_index,
            "prev_log_term": self.prev_log_term,
            "entries": [entry.as_dict() for entry in self.entries],
            "leader_commit_index": self.leader_commit_index,
        }

    def serialize(self) -> bytes:
        return json.dumps(self.as_dict()).encode("utf-8")

    @classmethod
    def from_dict(cls, data: dict) -> "AppendEntries":
        entries = data.pop("entries", [])
        return AppendEntries(**data, entries=[LogEntry(**d) for d in entries])


@dataclass
class AppendEntriesResponse:
    term: int
    success: bool
    match_index: int

    def as_dict(self):
        return {
            "term": self.term,
            "success": self.success,
            "match_index": self.match_index,
        }

    def serialize(self) -> bytes:
        return json.dumps(self.as_dict()).encode("utf-8")


@dataclass
class RequestVote:
    term: int
    candidate_id: NodeID
    last_log_index: int
    last_log_term: int

    def as_dict(self):
        return {
            "term": self.term,
            "candidate_id": self.candidate_id,
            "last_log_index": self.last_log_index,
            "last_log_term": self.last_log_term,
        }

    def serialize(self) -> bytes:
        return json.dumps(self.as_dict()).encode("utf-8")


@dataclass
class RequestVoteResponse:
    term: int
    vote_granted: bool

    def as_dict(self):
        return {
            "term": self.term,
            "vote_granted": self.vote_granted,
        }

    def serialize(self) -> bytes:
        return json.dumps(self.as_dict()).encode("utf-8")


@dataclass
class AddCommand(RaftMessage):
    # it's a little weird to have a 'destination' here, but convenient
    dest: NodeID
    command: str


@dataclass
class ClockTick(RaftMessage):
    dest: NodeID
    # would this be a good idea?
    # amount: float
    pass
