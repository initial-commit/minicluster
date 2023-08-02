import os
import stat
import collections
import psutil
import logging
import glob
import sys
import tarfile
import pyzstd
from contextlib import contextmanager
import inspect
import threading
import select
import pathlib

_true_set = {'yes', 'true', 't', 'y', '1'}
_false_set = {'no', 'false', 'f', 'n', '0'}


def str2bool(value, raise_exc=False):
    if isinstance(value, str) or sys.version_info[0] < 3 and isinstance(value, basestring):
        value = value.lower()
        if value in _true_set:
            return True
        if value in _false_set:
            return False

    if raise_exc:
        raise ValueError('Expected "%s"' % '", "'.join(_true_set | _false_set))
    return None


def str2bool_exc(value):
    return str2bool(value, raise_exc=True)


def path_stat(path, logger=None):
    try:
        s_obj = os.stat(path)
        modes = {k: getattr(stat, k) for k in dir(stat) if k.startswith(('S_IS', 'S_IMODE', 'S_IFMT', 'filemode')) and callable(getattr(stat, k))}
        modes = {k: v(s_obj.st_mode) for k, v in modes.items()}
        props = {k: getattr(stat, k) for k in dir(stat) if k.startswith(('ST_', )) and isinstance(getattr(stat, k), int)}
        props = {k: s_obj[v] for k, v in props.items()}
        return {**modes, **props}
    except:
        if logger:
            logger.debug(f"could not stat file {path=}", exc_info=True)
        return {}


def nbd_block_devices():
    logger = logging.getLogger(__name__)
    devices_and_partitions = {}

    util_parts = psutil.disk_partitions()
    mountpoint_map = {d.mountpoint: d.device for d in util_parts}
    device_map = {d.device: d.mountpoint for d in util_parts}
    mountpoint_util_parts_map = {util_parts[i].mountpoint: i
                                 for i in range(len(util_parts))}
    device_util_parts_map = {util_parts[i].device: i
                             for i in range(len(util_parts))}
    logger.debug(f"{util_parts=}")
    logger.debug(f"{mountpoint_map=}")
    logger.debug(f"{device_map=}")
    logger.debug(f"{mountpoint_util_parts_map=}")
    logger.debug(f"{device_util_parts_map=}")
    Disk = collections.namedtuple("Disk", [
        "name", "partitions", "mountpoints"]
        )
    for blockdev_stat in glob.glob('/sys/block/*/stat'):
        blockdev_dir = blockdev_stat.rsplit('/', 1)[0]
        name = blockdev_dir.rsplit('/', 1)[-1]
        if not name.startswith('nbd'):
            continue
        partitions = []
        mountpoints = []
        for part_stat in glob.glob(blockdev_dir + '/*/stat'):
            p_name = part_stat.rsplit('/', 2)[-2]
            partitions.append(p_name)
            devpath = f"/dev/{p_name}"
            if devpath in device_util_parts_map:
                i = device_util_parts_map[devpath]
                mountpoints.append(util_parts[i].mountpoint)
        d = Disk(name, partitions, mountpoints)
        devices_and_partitions[name] = d
    return devices_and_partitions


def get_unused_nbd_device():
    devices = nbd_block_devices()
    nbd_dev = None
    for d, i in devices.items():
        nbd_dev = d
        if len(i.mountpoints) == 0:
            break
    return nbd_dev


def get_current_nbd_disk(blk_info, mountpoint):
    for blockdev in blk_info['blockdevices']:
        if 'children' not in blockdev:
            continue
        for c in blockdev['children']:
            if mountpoint in c['mountpoints']:
                return blockdev['name']
    return None


def get_current_nbd_mountpoints(blk_info, mountpoint):
    blk = get_current_nbd_disk(blk_info, mountpoint)
    mnt = []
    for blockdev in blk_info['blockdevices']:
        if blockdev['name'] != blk:
            continue
        for c in blockdev['children']:
            for m in c['mountpoints']:
                if m and m.startswith(mountpoint):
                    mnt.append(m)
    return mnt


def interpolate_string(s, MINICLUSTER):
    return s.format(**MINICLUSTER._asdict())


class ZstdTarFile(tarfile.TarFile):
    def __init__(self, name, mode='r', *, level_or_option=None, zstd_dict=None, **kwargs):
        self.zstd_file = pyzstd.ZstdFile(name, mode,
                                  level_or_option=level_or_option,
                                  zstd_dict=zstd_dict)
        try:
            super().__init__(fileobj=self.zstd_file, mode=mode, **kwargs)
        except:
            self.zstd_file.close()
            raise

    def close(self):
        try:
            super().close()
        finally:
            self.zstd_file.close()


@contextmanager
def pushd(new_dir):
    previous_dir = os.getcwd()
    os.chdir(new_dir)
    try:
        yield
    finally:
        os.chdir(previous_dir)


def get_linenumber():
    cf = inspect.currentframe()
    return cf.f_back.f_lineno


class PipeTailer(threading.Thread):
    pipes = None
    logger = None

    def __init__(self, pipes, logger, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pipes = pathlib.Path(pipes)
        # TODO: add names of the pipe to thread and to logger
        self.logger = logger.getChild(self.__class__.__name__)
        assert self.pipes.exists(), f"Pipe {pipes} does not exist"

    def run(self):
        poller = select.epoll()
        fo = open(self.pipes, 'rb', 0)
        file_obj = {}
        file_obj[fo.fileno()] = fo
        poller.register(fo)
        keep_polling = True
        while keep_polling:
            events = poller.poll()
            for fd, evt in events:
                if evt & select.EPOLLIN:
                    data = file_obj[fd].read(2**13)
                    try:
                        data = data.decode('utf-8')
                    except UnicodeDecodeError:
                        pass
                    print(data, end='')
                if evt & select.EPOLLOUT:
                    print("POLLOUT")
                if evt & select.EPOLLERR:
                    print("POLLERR")
                if evt & select.EPOLLPRI:
                    print("pri")
                if evt & select.EPOLLHUP:
                    file_obj[fd].close()
                    keep_polling = False
                    break
                if evt & select.EPOLLET:
                    print("let")
                if evt & select.EPOLLONESHOT:
                    print("shot")
                if evt & select.EPOLLEXCLUSIVE:
                    print("excl")
                if evt & select.EPOLLRDHUP:
                    print("rdhup")
                if evt & select.EPOLLRDNORM:
                    print("rdnorm")
                if evt & select.EPOLLRDBAND:
                    print("rdband")
                if evt & select.EPOLLWRNORM:
                    print("wrnorm")
                if evt & select.EPOLLWRBAND:
                    print("wrband")
                if evt & select.EPOLLMSG:
                    print("msg")
