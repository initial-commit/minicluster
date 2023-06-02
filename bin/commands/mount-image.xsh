#!/usr/bin/env xonsh

d=p"$XONSH_SOURCE".resolve().parent; source f'{d}/bootstrap.xsh'
MINICLUSTER.ARGPARSE.add_argument('--handle', required=True)
MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import os
import logging
import cluster.functions

cwd = MINICLUSTER.CWD_START

logger = logging.getLogger(__name__)
handle = MINICLUSTER.ARGS.handle

disk_file = f"{cwd}/{handle}.qcow2"
mountpoint = f"{cwd}/{handle}"

mkdir -p @(mountpoint)

$RAISE_SUBPROC_ERROR = True
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
	mountpoint,
]

guestmount @(mount_args)
