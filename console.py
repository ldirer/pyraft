import logging
import threading
import traceback

from raft_server import RaftServer
from raftconfig import SERVERS
from raftconfig import NodeRole
from raftlog import log_to_str

ALLOWED_ROLES = {"follower", "leader", "candidate"}


def console(nodenum: int, role: str):
    if role not in ALLOWED_ROLES:
        print("wrong role, allowed: ", ALLOWED_ROLES)
        exit(1)

    role: NodeRole
    node = RaftServer(
        nodenum,
        role=role,
        configuration=SERVERS,
        application_execute_command=lambda command: print("executing command ", command),
    )
    t = threading.Thread(target=node.start)
    t.start()
    while True:
        cmd = input(f"Node {nodenum} ({node.role})> ").strip()
        # noinspection PyBroadException
        try:
            if cmd == "log":
                raft_log = node.logic.state.log
                print(f"log: '{log_to_str(raft_log)}' ({raft_log.last_log_index()} elements)")
            elif cmd.startswith("state"):
                eval_str = f"node.logic.{cmd}"
                print(f"state: '{eval(eval_str)}'")
            elif cmd.startswith("node"):
                print(f"{eval(cmd)}")
            elif cmd == "logging on":
                logging.disable(level=logging.NOTSET)
            elif cmd == "logging off":
                logging.disable(level=logging.INFO)
            elif cmd.startswith("command"):
                _, raft_command = cmd.split(maxsplit=1)
                node.add_command(raft_command)
            else:
                print("unknown command")
                continue
        except Exception:
            print(traceback.format_exc())
            continue

    t.join()


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("usage: python <filename> <nodeID> <role>\n" "you might want to use rlwrap as well for nicer input.")
    console(int(sys.argv[1]), sys.argv[2])
