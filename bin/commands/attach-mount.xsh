#!/usr/bin/env xonsh

if __name__ == '__main__':
    d=p"$XONSH_SOURCE".resolve().parent; source f'{d}/bootstrap.xsh'
    source @(f'{d}/instance-shell.xsh')
    MINICLUSTER.ARGPARSE.add_argument('--name', required=True)
    MINICLUSTER.ARGPARSE.add_argument('--dir_outside', required=True)
    MINICLUSTER.ARGPARSE.add_argument('--dir_inside', required=True)
    MINICLUSTER.ARGPARSE.add_argument('--tag', required=True)
    MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import os
import sys
import time
import grp
import logging
import cluster.qmp
import cluster.hypervisor
import pathlib

def wait_for_files(logger, wait_files, timeout):
    success_count = 0
    shown = False
    prev_wait = time.time()
    while success_count < len(wait_files):
	success_count = sum([1 if v.exists() else 0 for v in wait_files])
	if time.time() - prev_wait > timeout:
	    break
	if success_count < len(wait_files):
	    if not shown:
		logger.info(f"waiting for files {wait_files}")
		shown = True
	    if time.time() - prev_wait > timeout:
		break
	    time.sleep(0.1)
    return success_count

def command_attach_mount_xsh(cwd, logger, name, dir_outside, dir_inside, tag):
    s = f"{cwd}/qga-{name}.sock"
    agent = cluster.qmp.Connection(s, logger)
    s = f"{cwd}/monitor-{name}.sock"
    hypervisor = cluster.hypervisor.HypervisorConnection(s, logger)
    group_name = grp.getgrgid(os.getgid()).gr_name
    uid = os.getuid()
    # TODO: improve by reading from /etc/subuid and subgid
    m_param = f"b:{uid}:100000:65536"
    s_path = pathlib.Path(f"{cwd}/vfsd-{name}-{tag}.sock")
    pid_path = pathlib.Path(f"{s_path}.pid")
    wait_files = [s_path, pid_path]
    success = True
    for f in wait_files:
	if f.exists():
	    logger.error(f"File exists before mounting: {f}")
	    success = False
    if not success:
	return False
    # TODO: it is actually possible to use just virtiofsd
    lxc-usernsexec -s -m @(m_param) -- /usr/lib/virtiofsd --socket-group @(group_name) --socket-path @(s_path) --shared-dir @(dir_outside) --announce-submounts --sandbox none &
    success_count = wait_for_files(logger, wait_files, 5)
    if success_count != 2:
	logger.error("virtiofsd did not start correctly")
	return False
    success = hypervisor.add_chardev(f"{name}-{tag}", f"{s_path}")
    if not success:
	return False
    success = hypervisor.add_virtiofs_device(1024, f"{name}-{tag}", tag)
    if not success:
	return False
    (success, st) = command_instance_shell_simple_xsh(cwd, logger, name, f"mkdir -p {dir_inside}")
    if not success:
	logger.error("could not make directory inside")
	return False
    (success, st) = command_instance_shell_simple_xsh(cwd, logger, name, f"mount -t virtiofs {tag} {dir_inside}")
    if not success:
	logger.error("command failed: mount -t virtiofs {tag} {dir_inside}")
	logger.error(f"{st=}")
	return False
    return True

if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    cwd = MINICLUSTER.CWD_START
    name = MINICLUSTER.ARGS.name
    dir_outside = MINICLUSTER.ARGS.dir_outside
    dir_inside = MINICLUSTER.ARGS.dir_inside
    tag = MINICLUSTER.ARGS.tag

    $RAISE_SUBPROC_ERROR = True
    success = command_attach_mount_xsh(cwd, logger, name, dir_outside, dir_inside, tag)
    if not success:
	sys.exit(1)
