import logging
import os
import time
import traceback
from queue import Queue
from threading import Thread

from raftconfig import CLOCK_TICK_INTERVAL_S
from raftconfig import NodeID
from raftconfig import NodeRole
from raftlogic import RaftLogic
from raftnet import RaftNet
from rpc import RPC
from rpc import AddCommand
from rpc import ClockTick
from rpc import RaftMessage

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class RaftServer:
    def __init__(
        self,
        node_id: NodeID,
        role: NodeRole,
        configuration: dict[NodeID, tuple[str, int]],
        application_execute_command=None,
    ):
        self.node_id = node_id
        self.logic = RaftLogic(node_id=node_id, role=role, node_ids=list(configuration.keys()))
        self.network = RaftNet(nodenum=node_id, configuration=configuration)

        self.queue: Queue[RaftMessage] = Queue()
        self.application_execute_command = application_execute_command or (lambda command: ...)

    def __repr__(self):
        return f"RaftServer(node_id={self.node_id}, role={self.role})"

    @property
    def role(self) -> NodeRole:
        return self.logic.role

    def add_command(self, command: str):
        # I think here it's fine to crash if not leader.
        assert self.logic.role == "leader"
        self.queue.put(AddCommand(command=command, dest=self.node_id))

    def update_followers(self):
        messages = self.logic.update_followers()
        for rpc in messages:
            self.send(rpc)

    def receive(self):
        while True:
            msg = self.network.receive()
            try:
                rpc = RPC.deserialize(msg)
                if rpc.dest != self.node_id:
                    logger.error("Received a message that was not destined to us :O :O. That's a bug!")
                    continue

                self.queue.put(rpc)

            except Exception as e:
                logger.exception(e)
                # attempt at making it so that not every node needs to be restarted when there's an error.
                logger.info("Sleeping a few seconds and resuming receiving")
                time.sleep(3)

    def start(self):
        threads = [
            Thread(target=self.schedule_clock_ticks),
            Thread(target=self.process_queue),
            Thread(target=self.receive),
        ]

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    def schedule_clock_ticks(self):
        while True:
            self.queue.put(ClockTick(dest=self.node_id))
            time.sleep(CLOCK_TICK_INTERVAL_S)

    def process_queue(self):
        # noinspection PyBroadException
        try:
            while True:
                event = self.queue.get()
                if not isinstance(event, ClockTick):
                    logger.info("process queue: %s", event)

                # only *one* place where we process RPCs: means we don't need to worry about locking as much
                self.process_event(event)
        except Exception:
            print(traceback.format_exc())
            os._exit(1)

    def become_leader(self):
        self.logic.convert_to("leader")

    def send(self, rpc: RPC):
        # at the time of writing this is a blocking call: this is not what we want at all!
        # this might block the all-important loop processing incoming messages.
        # Or rather this *will block* as soon as there are connection difficulties.
        self.network.send(rpc.dest, rpc.serialize())

    def process_event(self, message):
        responses = self.logic.handle_message(message)
        for response in responses:
            self.send(response)

        # Notify the application about committed commands
        while self.logic.state.last_applied < self.logic.state.commit_index:
            # we should not need a lock here but maybe we want to make sure we don't run two threads by accident?
            # could be a global lock on process event. Or even not a lock. Just a safety.
            self.logic.state.last_applied += 1
            # Question: could we ever get into a state where the commit index is outside our current log?
            self.execute_command(self.logic.state.log[self.logic.state.last_applied].command)

    def execute_command(self, command):
        self.application_execute_command(command)
