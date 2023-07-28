#!/usr/bin/env xonsh

if __name__ == '__main__':
    d=p"$XONSH_SOURCE".resolve().parent; source @(f'{d}/bootstrap.xsh')
    MINICLUSTER.ARGPARSE.add_argument('--handle', required=True)
    MINICLUSTER.ARGPARSE.add_argument('--ro', action='store', type=str, default=None)
    MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import os
import logging

def command_mount_image_xsh(cwd, logger, handle, readonly=None):
    disk_file = f"{cwd}/{handle}.qcow2"
    mountpoint = f"{cwd}/{handle}"
    pidfile = f'{cwd}/guestmount-{handle}.pid'
    if readonly:
        mountpoint = f"{cwd}/{handle}-{readonly}"
        pidfile = f'{cwd}/guestmount-{handle}-{readonly}-ro.pid'

    mkdir -p @(mountpoint)

    uid=os.getuid()
    gid=os.getgid()

    mount_args = [
        '-o', 'allow_other',
        '-o', f'uid={uid}', '-o', f'gid={gid}',
        '-o', 'sync_read',
        #'-o', 'direct_io',
        '-o', 'kernel_cache',
        '-o', 'max_write=10',
        '--dir-cache-timeout', '1',
        '-o', 'attr_timeout=1',
        '--pid-file', pidfile,
        #'--no-fork', '--verbose', '--trace',
        '-a', disk_file,
        # TODO: get disk layout from disk specification
        '-m', '/dev/sda2:/',
        '-m', '/dev/sda1:/boot',
    ]
    if readonly:
        mount_args.append('--ro')
    mount_args.append(mountpoint)

    logger.info(f"mounting {disk_file} at {mountpoint}")

    guestmount @(mount_args)
    return mountpoint

if __name__ == '__main__':
    cwd = MINICLUSTER.CWD_START
    logger = logging.getLogger(__name__)
    handle = MINICLUSTER.ARGS.handle
    readonly = MINICLUSTER.ARGS.ro
    $RAISE_SUBPROC_ERROR = True
    command_mount_image_xsh(cwd, logger, handle, readonly)
