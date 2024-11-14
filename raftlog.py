import json
from dataclasses import dataclass


@dataclass
class LogEntry:
    command: str
    term: int

    def as_dict(self) -> dict:
        return {
            "command": self.command,
            "term": self.term,
        }

    def serialize(self) -> bytes:
        return json.dumps(self.as_dict()).encode("utf-8")


class RaftLog:
    """This does not concern itself with *all* the rules around the append entries RPC.
    Things like: 'reply false if term < currentTerm' are out of scope.

    In figure 2, this is responsible for steps 2, 3, and 4 of "receiver implementation" for AppendEntries.
    """

    def __init__(self, log: list[LogEntry] | None = None):
        # workaround 1-based indexing used in the paper by adding a fake initial entry.
        # makes everything so much simpler to reason about.
        self.log = (
            [LogEntry(command="placeholder", term=-1), *log]
            if log is not None
            else [LogEntry(command="placeholder", term=-1)]
        )

    def last_log_index(self) -> int:
        return len(self.log) - 1

    def __getitem__(self, index: int | slice):
        if isinstance(index, int) and index < 0:
            # better crash than do something hard to debug
            raise ValueError("index should not be negative")
        return self.log[index]

    def append_as_leader(self, entry: LogEntry):
        self.log.append(entry)

    def append_new_command_as_leader(self, leader_term, command):
        self.log.append(LogEntry(command=command, term=leader_term))

    def append_entries(self, prev_index: int, prev_term: int, entries: list[LogEntry]) -> bool:
        if prev_index >= len(self.log):
            return False

        if prev_index != -1 and self.log[prev_index].term != prev_term:
            return False

        # "if an existing entry conflicts with a new one (same index, different terms),
        # delete the existing entry and all that follow it."
        for entry_idx, entry in enumerate(entries, start=0):
            # compute index for corresponding element of the log
            log_idx = (prev_index + 1) + entry_idx
            if log_idx >= len(self.log):
                self.log.extend(entries[entry_idx:])
                break

            if self.log[log_idx].term != entry.term:
                self.log = self.log[:log_idx]
                self.log.extend(entries[entry_idx:])
                break

        return True

    def __repr__(self):
        return f"RaftLog(log={self.log})"

    def is_more_up_to_date(self, other_last_index, other_last_term):
        """
        5.4.1 (end of section)
        If the logs have last entries with different terms, then the log with the later term is more up-to-date.
        If the logs end with the same term, then whichever log is longer is more up-to-date.
        """
        last_term = self.log[-1].term
        return last_term > other_last_term or (
            last_term == other_last_term and self.last_log_index() > other_last_index
        )


def log_to_str(raft_log: RaftLog) -> str:
    """Debug/testing utility"""
    # remove the fake log entry when converting to string
    if all(entry.term < 10 for entry in raft_log.log):
        return "".join([str(entry.term) for entry in raft_log.log[1:]])
    else:
        # not as nice but less ambiguous for when one wants to read very long logs.
        return ".".join([str(entry.term) for entry in raft_log.log[1:]])
