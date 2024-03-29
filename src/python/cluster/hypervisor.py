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
    events_buffer = []

    def __init__(self, sock, logger):
        self.logger = logger.getChild(self.__class__.__name__)
        self.sock_path = sock
        exists = pathlib.Path(self.sock_path).exists()
        assert exists, "qemu monitor socket exists"
        resp = self.recv_json()
        self.logger.debug(f"{resp=}")
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
        self.logger.info(f"sending set_link: false")
        resp = self.send_recv(payload)
        self.logger.info(f"received set_link")
        self.logger.info(f"{resp=}")
        if 'return' not in resp:
            self.logger.error("error response, see below original payload and response")
            self.logger.error(f"{payload=}")
            self.logger.error(f"{resp=}")
        return 'return' in resp

    def add_chardev(self, id, path):
        payload = {
            "execute": "chardev-add",
            "arguments": {
                "id": id,
                "backend": {
                    "type": "socket",
                    "data": {
                        "addr": {
                            "type": "unix",
                            "data": {
                                "path": path,
                            },
                        },
                        "server": False,
                        #"logfile": f"{path}.log", # stored the binary data transferred (?)
                        "reconnect": 1,
                    },
                },
            },
        }
        self.logger.debug(f"chardev-add: {payload=}")
        resp = self.send_recv(payload)
        self.logger.debug(f"chardev-add {resp=}")
        if 'return' not in resp:
            self.logger.error("error response, see below original payload and response")
            self.logger.error(f"{payload=}")
            self.logger.error(f"{resp=}")
        return 'return' in resp

    def remove_chardev(self, id):
        payload = {
            "execute": "chardev-remove",
            "arguments": {
                "id": id,
            }
        }
        self.logger.debug(f"chardev-remove: {payload=}")
        resp = self.send_recv(payload)
        self.logger.debug(f"chardev-remove {resp=}")
        if 'return' not in resp:
            self.logger.error("error response, see below original payload and response")
            self.logger.error(f"{payload=}")
            self.logger.error(f"{resp=}")
        return 'return' in resp

    def add_virtiofs_device(self, queue_size, chardev, tag):
        payload = {
            "execute": "device_add",
            "arguments": {
                "driver": "vhost-user-fs-pci",
                "queue-size": queue_size,
                "chardev": chardev,
                "tag": tag,
                "bus": "pci.6",
                "id": f"vfsd-{tag}",
            },
        }
        self.logger.debug(f"device_add: {payload=}")
        resp = self.send_recv(payload)
        self.logger.debug(f"device_add virtiofs {resp=}")
        if 'return' not in resp:
            self.logger.error("error response, see below original payload and response")
            self.logger.error(f"{payload=}")
            self.logger.error(f"{resp=}")
        return 'return' in resp

    def remove_virtiofs_device(self, tag):
        payload = {
            "execute": "device_del",
            "arguments": {
                "id": f"vfsd-{tag}",
            }
        }
        # TODO: read docs, we need to handle events DEVICE_DELETED and DEVICE_UNPLUG_GUEST_ERROR
        self.logger.debug(f"device_del: {payload=}")
        resp = self.send_recv(payload)
        self.logger.debug(f"device_del virtiofs {resp=}")
        if 'return' not in resp:
            self.logger.error("error response, see below original payload and response")
            self.logger.error(f"{payload=}")
            self.logger.error(f"{resp=}")
        data = self.get_one_event_blocking()
        self.logger.info(f"received one event: {data=}")
        return 'return' in resp

    def query_schema(self):
        payload = {
            "execute": "query-qmp-schema",
        }
        resp = self.send_recv(payload)
        self.logger.info(f"SCHEMA {resp=}")
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

    def get_one_event_blocking(self):
        if len(self.events_buffer) == 0:
            return self.recv_json()
        else:
            return self.events_buffer.pop(0)

    def send_recv(self, payload):
        s = self._get_socket()
        payload = json.dumps(payload).encode('utf-8')
        self.logger.debug(payload)
        s.sendall(payload)
        return self.recv_json()

    def _raw_recv_json(self):
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

    def recv_json(self):
        return_received = False
        data = None
        while not return_received:
            json_data = self._raw_recv_json()
            if 'events' in json_data:
                self.events_buffer.append(json_data)
            else:
                return_received = True
                data = json_data
        return data
