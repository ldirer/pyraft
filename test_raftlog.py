from raftlog import LogEntry
from raftlog import RaftLog
from raftlog import log_to_str


def str_to_log(slog: str) -> RaftLog:
    return RaftLog(log=[LogEntry(command="", term=int(t)) for t in slog])


def test_append_empty_entries():
    rlog = RaftLog(log=[LogEntry(command="set x 12", term=0)])
    assert rlog.append_entries(prev_index=1, prev_term=0, entries=[]) is True
    assert rlog.log[1:] == [LogEntry(command="set x 12", term=0)]

    # term does not match
    assert rlog.append_entries(prev_index=1, prev_term=1, entries=[]) is False


def test_raft_log_empty():
    rlog = RaftLog(log=[])
    assert rlog.append_entries(prev_index=0, prev_term=-1, entries=[LogEntry(command="set x 1", term=0)]) is True
    assert rlog.log[1:] == [LogEntry(command="set x 1", term=0)]

    rlog = RaftLog(log=[LogEntry(command="set x 12", term=0)])
    assert rlog.append_entries(prev_index=0, prev_term=-1, entries=[LogEntry(command="set x 1", term=1)]) is True
    assert rlog.log[1:] == [LogEntry(command="set x 1", term=1)], "conflicting log entry should have been replaced"


def test_idempotence():
    rlog = RaftLog(log=[LogEntry(command="set x 1", term=0)])
    # add another entry twice to test idempotence
    assert rlog.append_entries(prev_index=1, prev_term=0, entries=[LogEntry(command="set y 2", term=1)]) is True
    assert rlog.log[1:] == [LogEntry(command="set x 1", term=0), LogEntry(command="set y 2", term=1)]
    assert rlog.append_entries(prev_index=1, prev_term=0, entries=[LogEntry(command="set y 2", term=1)]) is True
    assert rlog.log[1:] == [
        LogEntry(command="set x 1", term=0),
        LogEntry(command="set y 2", term=1),
    ], "'append entries' should be idempotent"


def test_no_holes():
    rlog = RaftLog(log=[LogEntry(command="set x 1", term=0)])
    assert rlog.append_entries(prev_index=8, prev_term=0, entries=[LogEntry(command="set y 2", term=0)]) is False


def test_does_not_discard_compatible_entries():
    # I think this is actually critical.
    # imagine an old append entries retry comes in: we don't want it to 'undo' valid parts of the log.
    rlog = str_to_log("122333")
    assert rlog.append_entries(prev_index=3, prev_term=2, entries=[LogEntry(command="", term=3)]) is True
    assert log_to_str(rlog) == "122333"


def test_some_compatible_some_incompatible_entries():
    # looking for off-by-one errors
    rlog = str_to_log("122333")
    assert (
        rlog.append_entries(
            prev_index=3,
            prev_term=2,
            entries=[LogEntry(command="", term=3), LogEntry(command="", term=3), LogEntry(command="", term=5)],
        )
        is True
    )
    assert log_to_str(rlog) == "122335"


def test_raft_log_figure_7():
    # z is leader ('l' gives linting errors and I can't be bothered to lookup how to ignore them)
    z = "1114455666"
    a = "111445566"
    # b = "1114"
    c = "11144556666"
    d = "111445566677"
    e = "1114444"
    # f = "11122233333"

    a = str_to_log(a)
    # b = str_to_log(b)
    c = str_to_log(c)
    d = str_to_log(d)
    e = str_to_log(e)
    # f = str_to_log(f)

    assert a.append_entries(prev_index=len(z), prev_term=6, entries=[LogEntry(command="", term=8)]) is False
    assert (
        a.append_entries(
            prev_index=len(z) - 1, prev_term=6, entries=[LogEntry(command="", term=6), LogEntry(command="", term=8)]
        )
        is True
    )
    assert log_to_str(a) == "11144556668"

    assert c.append_entries(prev_index=len(z), prev_term=6, entries=[LogEntry(command="", term=8)]) is True
    assert log_to_str(c) == "11144556668"

    assert d.append_entries(prev_index=len(z), prev_term=6, entries=[LogEntry(command="", term=8)]) is True
    assert log_to_str(d) == "11144556668"

    assert e.append_entries(prev_index=len(z), prev_term=6, entries=[LogEntry(command="", term=8)]) is False
    assert (
        e.append_entries(
            prev_index=5,
            prev_term=4,
            entries=[
                LogEntry(command="", term=5),
                LogEntry(command="", term=5),
                LogEntry(command="", term=6),
                LogEntry(command="", term=6),
                LogEntry(command="", term=6),
                LogEntry(command="", term=8),
            ],
        )
        is True
    )
    assert log_to_str(e) == "11144556668"


def gt(log1, log2) -> bool:
    return log1.is_more_up_to_date(log2.last_log_index(), log2[log2.last_log_index()].term)


def test_raft_log_comparison():
    log1 = str_to_log("1122")
    log2 = str_to_log("1122")
    assert gt(log1, log2) is False
    assert gt(log2, log1) is False

    log1 = str_to_log("112")
    log2 = str_to_log("1122")
    assert gt(log1, log2) is False
    assert gt(log2, log1) is True

    log1 = str_to_log("1123")
    log2 = str_to_log("1122")
    assert gt(log1, log2) is True
    assert gt(log2, log1) is False

    log1 = str_to_log("11222")
    log2 = str_to_log("1122")
    assert gt(log1, log2) is True
    assert gt(log2, log1) is False

    log1 = str_to_log("3")
    log2 = str_to_log("11222222")
    assert gt(log1, log2) is True
    assert gt(log2, log1) is False

    log1 = str_to_log("111445566")
    log2 = str_to_log("1114")
    assert gt(log1, log2) is True
    assert gt(log2, log1) is False
