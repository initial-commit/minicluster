import socket
import json
import os
import shlex
import base64
import time
import pathlib
import select
import stat


class CachedViaConstructorMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        keys = [str(cls)]
        keys.extend([str(k) for k in args])
        keys.extend([str(k)+':'+str(v) for k, v in kwargs.items()])
        keys = tuple(keys)
        if keys not in cls._instances:
            cls._instances[keys] = super(CachedViaConstructorMeta, cls).__call__(*args, **kwargs)

        return cls._instances[keys]


class Connection(object, metaclass=CachedViaConstructorMeta):
    _socket = None
    _buf = None
    _info = None
    logger = None

    def __init__(self, sock, logger):
        self.logger = logger.getChild(self.__class__.__name__)
        sock = pathlib.Path(sock).absolute()
        exists = sock.exists()
        self.logger.debug(f"starting for sock {sock=} {exists=}")
        assert exists, f"Socket for qmp agent does not exist: {sock}"
        self.sock_path = str(sock)
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
            assert isinstance(input_data, bytes), f"input data is in the wrong format: {input_data=}"
            input_data = base64.b64encode(input_data).decode()
            guest_args['input-data'] = input_data
        self.logger.debug(f"executing command {cmd=}", extra=guest_args)
        resp = self._send_recv_rountrip("guest-exec", **guest_args)
        if 'return' not in resp:
            self.logger.debug(f"return not in response {resp=}")
            return False
        return resp['return']['pid']

    def guest_exec_status(self, pid: int):
        resp = self._send_recv_rountrip("guest-exec-status", pid=pid)
        if 'return' not in resp:
            self.logger.error(f"return not in response {resp=}", extra={'resp': resp})
            return None
        return resp['return']

    def guest_exec_wait(self, cmd, input_data=None, capture_output=True, env=[], interval=0.005, out_encoding='utf-8'):
        self.logger.debug(f"preparing to execute cmd {cmd=}", extra={'input_data': input_data})
        pid = self.guest_exec(cmd, input_data, capture_output, env)
        assert isinstance(pid, int), "pid must be int, value: {pid=}"
        status = self.guest_exec_status(pid)
        if 'exited' not in status:
            self.logger.error("command returned {cmd=} {status=}")
        self.logger.debug(f"command returned {cmd=} {status=}", extra={'cmd': cmd, 'status': status})
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
            status['out-data'] = status['out-data'].decode(out_encoding, 'backslashreplace')
            status['err-data'] = status['err-data'].decode(out_encoding, 'backslashreplace')
        return status

    def guest_sync_pacman_databases(self):
        status = self.guest_exec_wait("pacman -Syy --noconfirm")
        self.logger.info(f"syncing db {status=}")
        return status

    def guest_has_package(self, pkgs):
        status = self.guest_exec_wait(f"pacman -Qi {pkgs}")
        self.logger.debug(f"pacman check if pkg exists {pkgs=} {status=}")
        return status['exitcode'] == 0

    def guest_install_package(self, pkgs):
        status = self.guest_exec_wait(f"pacman -Qi {pkgs}")
        if status['exitcode'] == 1:
            status = self.guest_exec_wait(f"pacman -S --noconfirm --overwrite '*' {pkgs}")
            assert status['exitcode'] == 0
            return True
        return False

    def path_stat(self, vm_path):
        # TODO: extract code automatically from functions.path_stat
        self.logger.debug(f"get stat of {vm_path=}")
        prog = (
            "import os\n"
            "import json\n"
            "import sys\n"
            "import stat\n"
            "try:\n"
            f"  s_obj = os.stat('{vm_path}')\n"
            "  modes = {k: getattr(stat, k) for k in dir(stat) if k.startswith(('S_IS', 'S_IMODE', 'S_IFMT', 'filemode')) and callable(getattr(stat, k)) }\n"
            "  modes = {k: v(s_obj.st_mode) for k,v in modes.items()}\n"
            "  props = {k: getattr(stat, k) for k in dir(stat) if k.startswith(('ST_', )) and isinstance(getattr(stat, k), int) }\n"
            "  props = {k: s_obj[v] for k,v in props.items()}\n"
            "  print(json.dumps({**modes, **props}))\n"
            "except:\n"
            "  print('{}')\n"
            "  sys.exit(1)\n"
        )
        stat_result = self.guest_exec_wait(["python", "-c", prog])
        if stat_result['exitcode']:
            self.logger.debug(f"could not get path stat: {stat_result=} for {vm_path=}")
            return None
        return json.loads(stat_result['out-data'])

    def make_symlink_vm(self, cwd, link, target_file):
        self.logger.debug(f"make symlink in {cwd=} from {link=} to {target_file=}")
        st = self.guest_exec_wait(["bash", '-c', f"cd {cwd} && ln -s {link} {target_file}"])
        return st['exitcode'] == 0

    def chmod(self, cmd_args):
        # generated via: for l in $(cat test.py | sed 's/    /\\\\t/g').splitlines(): print(f"\"{l.rstrip()}\\n\"")
        prog = (
            "import sys\n"
            "import os\n"
            "argv = sys.argv\n"
            "if '--' not in argv:\n"
            "\tsys.exit(1)\n"
            "argv = argv[argv.index('--')+1:]\n"
            "if len(argv) % 2 != 0:\n"
            "\tsys.exit(2)\n"
            "files = {}\n"
            "mode = 0o755\n"
            "for i in range(len(argv)):\n"
            "\tif i % 2 == 0:\n"
            "\t\tmode = argv[i]\n"
            "\telse:\n"
            "\t\tfiles[argv[i]] = mode\n"
            "for f, m in files.items():\n"
            "\ttry:\n"
            "\t\tm = int(m, 8)\n"
            "\t\tos.chmod(f, m)\n"
            "\texcept Exception:\n"
            "\t\traise Exception(f'failed chmod of {f=} to {m=}')"
        )
        #self.logger.info(prog)
        has_python = self.guest_exec_wait("which python")
        if has_python['exitcode'] == 0:
            cmd = ["python", "-c", prog, '--']
            cmd.extend(cmd_args)
            stat_result = self.guest_exec_wait(cmd)
            return stat_result['exitcode'] == 0
        else:
            mode = '644'
            has_failures = False
            for i in range(len(cmd_args)):
                if i % 2 == 0:
                    mode = cmd_args[i]
                    mode = oct(mode)[-3:]
                else:
                    fname = cmd_args[i]
                    fname = shlex.quote(fname)
                    cmd = f"chmod {mode} {fname}"
                    st = self.guest_exec_wait(cmd)
                    if st['exitcode'] != 0:
                        self.logger.error(f"failed: {cmd}")
                        has_failures = True
                    mode = '644'
            if has_failures:
                return False
            return True

    def write_to_vm(self, fp, vm_path):
        pos = fp.tell()
        fsize = fp.seek(0, os.SEEK_END)
        read_perc = 0
        read_perc_prev = 0
        fp.seek(pos, os.SEEK_SET)
        # TODO: check this, the behavior is strange
        # copy-files.xsh --from '{DIR_R}' --to t1:/root
        # resp = self.path_stat(vm_path)
        # is_overwrite = False
        # if not resp:
        #     is_overwrite = True
        # self.logger.info(f"will overwrite {vm_path=} inside: {is_overwrite}")
        # TODO: create a temporary file, then move
        resp = self._send_recv_rountrip("guest-file-open", path=vm_path, mode='wb')
        assert 'return' in resp, f"No return, instead: {resp=}"
        h = resp['return']
        written = 0
        read_size = 0
        self.logger.debug("starting read loop")
        while True:
            data = fp.read(16 * 1024 * 1024)
            if not data:
                self.logger.debug(f"no more data {data=}")
                break
            read_size += len(data)
            read_perc = int(read_size / fsize * 100)
            if read_perc - read_perc_prev >= 1 and read_perc < 100:
                self.logger.info(f"read from file {read_perc=}")
            read_perc_prev = read_perc
            data = base64.b64encode(data).decode('utf-8')
            kwargs = {'handle': h, 'buf-b64': data}
            resp = self._send_recv_rountrip("guest-file-write", **kwargs)
            written_chunk = resp['return']['count']
            written += written_chunk
        resp = self._send_recv_rountrip("guest-file-flush", handle=h)
        assert ('return' in resp and not resp['return'])
        # TODO: check that it was flushed properly
        resp = self._send_recv_rountrip("guest-file-close", handle=h)
        assert ('return' in resp and not resp['return'])
        # TODO: check that it was closed properly
        assert (read_size == written)
        return written

    def read_from_vm(self, fp, vm_path):
        resp = self._send_recv_rountrip("guest-file-open", path=vm_path, mode='rb')
        h = resp['return']
        seekinfo = self._send_recv_rountrip("guest-file-seek", handle=h, offset=0, whence=2)
        fsize = seekinfo['return']['position']
        self._send_recv_rountrip("guest-file-seek", handle=h, offset=0, whence=0)
        written = 0
        read_size = 0
        eof = False
        while not eof:
            resp = self._send_recv_rountrip("guest-file-read", handle=h, count=48 * 1024 * 1024)
            read_size += resp['return']['count']
            eof = resp['return']['eof']
            if not eof:
                data = base64.b64decode(resp['return']['buf-b64'])
                fp.write(data)
            read_perc = int(read_size / fsize * 100)
            self.logger.info(f"{read_perc=} {fsize=} {read_size=}")
        resp = self._send_recv_rountrip("guest-file-close", handle=h)
        assert ('return' in resp and not resp['return'])
        self.logger.info(f"{h=}")
        #guest-file-seek handle: offset: o whence: 2

    def unarchive_in_vm(self, vm_path):
        # TODO: detect command based on path
        cwd = str(pathlib.Path(vm_path).parent)
        resp = self.guest_exec_wait(["bash", "-c", f"cd {cwd} && tar xfz {vm_path} && rm {vm_path}"], env=[f"PWD={cwd}"])
        # TODO: return success

    def guest_read_file_out(self, vm_path):
        resp = self._send_recv_rountrip()

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
            assert self.sock_path is not None, f"No socket {self.sock_path=}"
            self.logger.debug(f"connecting socket {self.sock_path=}")
            self._socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            #self._socket.settimeout(1)
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

    def _send_raw(self, msg):
        s = self._get_socket()
        if 'execute' in msg and msg['execute'] in ['guest-sync-delimited', 'guest-sync']:
            self.logger.info("doing a qga sync")
        msg = json.dumps(msg).encode('utf-8')
        return s.sendall(msg)

    def _get_raw(self):
        s = self._get_socket()
        if not self._buf:
            self._buf = bytearray()

        c = None
        while c is None:
            #self.logger.debug(f"receiving one")
            c = s.recv(1)
            #self.logger.debug(f"received {c=}")
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
