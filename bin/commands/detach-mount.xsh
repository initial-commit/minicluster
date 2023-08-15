#!/usr/bin/env xonsh

if __name__ == '__main__':
    d=p"$XONSH_SOURCE".resolve().parent; source f'{d}/bootstrap.xsh'
    source @(f'{d}/instance-shell.xsh')
    MINICLUSTER.ARGPARSE.add_argument('--name', required=True)
    MINICLUSTER.ARGPARSE.add_argument('--tag', required=True)
    MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import os
import sys
import time
import grp
import logging
import cluster.qmp
import cluster.hypervisor

def command_detach_mount_xsh(cwd, logger, name, tag):
    s = f"{cwd}/qga-{name}.sock"
    agent = cluster.qmp.Connection(s, logger)
    s = f"{cwd}/monitor-{name}.sock"
    hypervisor = cluster.hypervisor.HypervisorConnection(s, logger)
    s_path = pf"{cwd}/vfsd-{name}-{tag}.sock"
    pid_path = pf"{s_path}.pid"
    (success, st) = command_instance_shell_simple_xsh(cwd, logger, name, f"umount -t virtiofs {tag}")
    logger.info(f"umount : {success=} {st=}")
    if not success:
	logger.error("could not umount inside VM")
	return False
    success = hypervisor.remove_virtiofs_device(tag)
    if not success:
	logger.error("could not remove virtiofs device")
	return False
    success = hypervisor.remove_chardev(f"{name}-{tag}")
    if not success:
	logger.error("could not remove chardev")
	return False
    still_open_count=int($(lsof -Fn @(s_path) | grep -P '^p[0-9]+$' | wc -l).rstrip())
    assert still_open_count == 0, f"Some processes still have socket open, execute to see: lsof {s_path}"
    s_path.unlink(True)
    pid_path.unlink(True)
    return True

if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    cwd = MINICLUSTER.CWD_START
    name = MINICLUSTER.ARGS.name
    tag = MINICLUSTER.ARGS.tag

    $RAISE_SUBPROC_ERROR = True
    success = command_detach_mount_xsh(cwd, logger, name, tag)
    if not success:
	sys.exit(1)
