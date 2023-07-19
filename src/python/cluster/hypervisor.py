import socket
import pathlib
import select
import time
import json


# {"execute": "query-stats", "arguments": {"target": "vm"}}
# {"execute": "query-stats", "arguments": {"target": "vcpu"}}
# {"execute": "human-monitor-command", "arguments": {"command-line": "info network"}}

class HypervisorConnection(object):
    _socket = None
    logger = None
    _buf = None

    def __init__(self, sock, logger):
        self.logger = logger.getChild(self.__class__.__name__)
        self.sock_path = sock
        exists = pathlib.Path(self.sock_path).exists()
        assert exists, "qemu monitor socket exists"
        resp = self.recv_json()
        self.logger.info(f"{resp=}")
        resp = self.send_recv({"execute": "qmp_capabilities"})
        self.logger.debug(f"{resp=}")

    def _get_socket(self):
        if not self._socket:
            self.logger.debug("connecting socket")
            self._socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            connect_stat = self._socket.connect(self.sock_path)
            poller = select.poll()
            poller.register(self._socket)
            not_ready = True
            while not_ready:
                self.logger.debug("polling for events")
                events = poller.poll()
                self.logger.debug(f"events received {events=}")
                for sock, evt in events:
                    if evt & select.POLLNVAL:
                        self.logger.debug("POLLNVAL")
                    if evt & select.POLLRDHUP:
                        self.logger.debug("POLLRDHUP")
                    if evt & select.POLLIN:
                        self.logger.debug("POLLIN")
                        not_ready = False
                        break
                    if evt & select.POLLOUT:
                        self.logger.debug("POLLOUT")
                        not_ready = False
                        break
                    if evt & select.POLLPRI:
                        self.logger.debug("POLLPRI")
                    if evt & select.POLLERR:
                        self.logger.debug("POLLERR")
                    if evt & select.POLLHUP:
                        self.logger.debug("POLLHUP")
                time.sleep(1)
            self.logger.debug(f"connecting socket {connect_stat=}")
        return self._socket

    def network_off(self):
        # TODO: get names of networks from monitor with "info network"
        payload = {
            "execute": "set_link",
            "arguments": {"name": "mynet0", "up": False}
        }
        resp = self.send_recv(payload)
        self.logger.info(f"{resp=}")
        assert 'return' in resp
        return True

    def network_on(self):
        payload = {
            "execute": "set_link",
            "arguments": {"name": "mynet0", "up": True}
        }
        resp = self.send_recv(payload)
        self.logger.debug(f"{resp=}")
        assert 'return' in resp
        return True

    def send_recv(self, payload):
        s = self._get_socket()
        payload = json.dumps(payload).encode('utf-8')
        self.logger.debug(payload)
        s.sendall(payload)
        return self.recv_json()

    def recv_json(self):
        s = self._get_socket()
        if not self._buf:
            self._buf = bytearray()
        c = None
        while c is None:
            c = s.recv(1)
            if c == b'{':
                op = 1
                self._buf += c
            else:
                c = None
        while op > 0:
            c = s.recv(1)
            if c == b'}':
                op -= 1
            if c == b'{':
                op += 1
            self._buf += c
            if op == 0:
                c = s.recv(2)
                if c == b'\r\n':
                    pass
                else:
                    raise Exception(f"unexpected chars left in response: {c=}")
        t = self._buf.decode('utf-8')
        self._buf = None
        return json.loads(t)
