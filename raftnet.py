import logging
from _socket import SO_REUSEADDR
from _socket import SOL_SOCKET
from socket import AF_INET
from socket import SOCK_STREAM
from socket import socket

from message import recv_message
from message import send_message
from raftconfig import NodeID

logger = logging.getLogger(__name__)


class RaftNet:
    """Minimalist implementation to make things work.
    Ex of basic optimization: reuse connections when sending messages.
    """

    def __init__(self, nodenum: NodeID, configuration: dict[NodeID, tuple[str, int]]):
        self.nodenum = nodenum
        self.configuration = configuration
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.listen()

    def listen(self):
        self.sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
        self.sock.bind(self.configuration[self.nodenum])
        self.sock.listen()

    def send(self, destination: NodeID, message: bytes):
        """Send a message to a specific node number"""
        dest_address = self.configuration[destination]
        sock = socket(AF_INET, SOCK_STREAM)
        try:
            sock.connect(dest_address)
            logger.info("sending to %s: %s", destination, message)
            send_message(sock, message)
        except ConnectionRefusedError:
            logger.error("connection refused on node %s", destination)
        except IOError as e:
            logger.exception("error sending message: ", e)
        finally:
            sock.close()

    def receive(self) -> bytes:
        """Receive and return any message that was sent to me"""
        client, addr = self.sock.accept()
        try:
            msg = recv_message(client)
            # logger.info("received: %s", msg)
            return msg
        except IOError as e:
            logger.exception("error receiving message: ", e)
            client.close()
