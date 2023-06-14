#!/usr/bin/env xonsh

if __name__ == '__main__':
    d=p"$XONSH_SOURCE".resolve().parent; source @(f'{d}/bootstrap.xsh')
    MINICLUSTER.ARGPARSE.add_argument('--handle', required=True)
    MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import os
import logging

def command_mount_image_xsh(cwd, logger, handle):
    disk_file = f"{cwd}/{handle}.qcow2"
    mountpoint = f"{cwd}/{handle}"

    mkdir -p @(mountpoint)

    uid=os.getuid()
    gid=os.getgid()

    mount_args = [
        '-o', 'allow_other',
        '-o', f'uid={uid}', '-o', f'gid={gid}',
        '--pid-file', f'/tmp/guestmount-{handle}.pid',
        #'--no-fork', '--verbose', '--trace',
        '-a', disk_file,
        '-m', '/dev/sda2:/',
        '-m', '/dev/sda1:/boot',
        mountpoint
    ]

    guestmount @(mount_args)

if __name__ == '__main__':
    cwd = MINICLUSTER.CWD_START
    logger = logging.getLogger(__name__)
    handle = MINICLUSTER.ARGS.handle
    $RAISE_SUBPROC_ERROR = True
    command_mount_image_xsh(cwd, logger, handle)
