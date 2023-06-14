import socket
import json
import os
import shlex
import base64
import time


class Connection(object):
    _socket = None
    _buf = None
    _info = None

    def __init__(self, sock):
        self.sock_path = sock
        resp = self._send_recv_rountrip("guest-sync-delimited", id=os.getpid())
        if 'return' not in resp or resp['return'] != os.getpid():
            raise Exception("could not sync with guest agent upon connecting")

    def ping(self):
        resp = self._send_recv_rountrip("guest-ping")
        if 'return' in resp and len(resp['return']) == 0:
            return True
        return False

    def guest_info(self):
        """
        Returns dict with two keys: version and supported_commands
        """
        if not self._info:
            self._info = self._guest_info()
        return self._info

    def guest_get_osinfo(self):
        resp = self._send_recv_rountrip("guest-get-osinfo")
        print(f"{resp=}")

    def guest_exec(self, cmd, input_data=None, capture_output=True, env=[]):
        if isinstance(cmd, str):
            cmd = shlex.split(cmd)
        if len(cmd) < 1:
            raise Exception("no command provided")
        args = []
        if len(cmd) > 1:
            args = cmd[1:]
        cmd = cmd[0]
        guest_args = {'path': cmd,
                      'arg': args,
                      'env': env,
                      'capture-output': capture_output}
        if input_data:
            input_data = base64.b64encode(input_data)
            guest_args['input-data'] = input_data
        resp = self._send_recv_rountrip("guest-exec", **guest_args)
        if 'return' not in resp:
            return False
        return resp['return']['pid']

    def guest_exec_status(self, pid):
        resp = self._send_recv_rountrip("guest-exec-status", pid=pid)
        if 'return' not in resp:
            return None
        return resp['return']

    def guest_exec_wait(self, cmd, input_data=None, capture_output=True, env=[], interval=0.1, out_encoding='utf-8'):
        pid = self.guest_exec(cmd, input_data, capture_output, env)
        status = self.guest_exec_status(pid)
        while not status['exited']:
            time.sleep(interval)
            status = self.guest_exec_status(pid)
        if 'out-data' in status:
            status['out-data'] = base64.b64decode(status['out-data'])
        else:
            status['out-data'] = b''
        if 'err-data' in status:
            status['err-data'] = base64.b64decode(status['err-data'])
        else:
            status['err-data'] = b''
        if out_encoding:
            status['out-data'] = status['out-data'].decode(out_encoding)
            status['err-data'] = status['err-data'].decode(out_encoding)
        return status

    def guest_get_users(self):
        resp = self._send_recv_rountrip("guest-get-users")
        print(f"{resp=}")

    def _guest_info(self):
        resp = self._send_recv_rountrip("guest-info")
        if 'return' not in resp:
            return False
        if 'version' not in resp['return']:
            return False
        if 'supported_commands' not in resp['return']:
            return False
        version = resp['return']['version']
        supported_commands = [c['name']
                              for c in resp['return']['supported_commands']
                              if c['enabled']]
        return {'version': version, 'supported_commands': supported_commands}

    def _send_recv_rountrip(self, execute, **kwargs):
        if kwargs:
            kwargs = {'arguments': kwargs}
        s = {"execute": execute, **kwargs}
        self._send_raw(s)
        resp = self._get_raw()
        return json.loads(resp)

    def _get_socket(self):
        if not self._socket:
            self._socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self._socket.connect(self.sock_path)
        return self._socket

    def _send_raw(self, msg):
        s = self._get_socket()
        msg = json.dumps(msg).encode('utf-8')
        return s.sendall(msg)

    def _get_raw(self):
        s = self._get_socket()
        if not self._buf:
            self._buf = bytearray()

        c = None
        while c is None:
            c = s.recv(1)
            op = 0
            if c in [b'{', b'\xff']:
                if c == b'{':
                    op = 1
                    self._buf += c
                if c == b'\xff':
                    c = None
            else:
                raise Exception(f"response start must be a json was {c=}")
        while op > 0:
            c = s.recv(1)
            if c == b'}':
                op -= 1
            if c == b'{':
                op += 1
            self._buf += c
            if op == 0:
                c = s.recv(1)
                if c == b'\n':
                    pass
                else:
                    raise Exception(f"unexpected char left in respose: {c=}")
        t = self._buf.decode('utf-8')
        self._buf = None
        return t
