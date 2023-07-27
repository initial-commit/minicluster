#!/usr/bin/env xonsh

if __name__ == '__main__':
	d=p"$XONSH_SOURCE".resolve().parent; source f'{d}/bootstrap.xsh'
	MINICLUSTER.ARGPARSE.add_argument('--handle', required=True)
	MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import logging
import psutil
import time
import os

def command_umount_image_xsh(cwd, logger, handle):
	mountpoint = f"{cwd}/{handle}"
	logger.info(f"unmounting {mountpoint}")

	pid = None
	for pidfile in [f"{cwd}/guestmount-{handle}.pid", f"{cwd}/guestmount-{handle}-ro.pid"]:
		if os.path.exists(pidfile):
			with open(pidfile, 'r') as f:
				pid = int(f.read().rstrip())
	if pid is None:
		logger.error(f"could not find pidfile for {mountpoint=}")
		return False
	guestunmount @(mountpoint)

	while pid and psutil.pid_exists(pid):
		logger.info("pid exists")
		time.sleep(0.2)
	return True

if __name__ == '__main__':
	cwd = MINICLUSTER.CWD_START

	logger = logging.getLogger(__name__)
	handle = MINICLUSTER.ARGS.handle

	$RAISE_SUBPROC_ERROR = True
	command_umount_image_xsh(cwd, logger, handle)
