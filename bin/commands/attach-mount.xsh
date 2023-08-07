#!/usr/bin/env xonsh

if __name__ == '__main__':
    d=p"$XONSH_SOURCE".resolve().parent; source f'{d}/bootstrap.xsh'
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

#s_path = f"{cwd}/vfsd-{name}-{tag}.sock"
#pid_path = f"{s_path}.pid"
#pkill -9 -F @(pid_path)

def command_attach_mount_xsh(cwd, logger, name, dir_outside, dir_inside, tag):
    s = f"{cwd}/qga-{name}.sock"
    agent = cluster.qmp.Connection(s, logger)
    s = f"{cwd}/monitor-{name}.sock"
    hypervisor = cluster.hypervisor.HypervisorConnection(s, logger)
    group_name = grp.getgrgid(os.getgid()).gr_name
    uid = os.getuid()
    # TODO: improve by reading from /etc/subuid and subgid
    m_param = f"b:{uid}:100000:65536"
    s_path = f"{cwd}/vfsd-{name}-{tag}.sock"
    pid_path = f"{s_path}.pid"
    lxc-usernsexec -s -m @(m_param) -- /usr/lib/virtiofsd --socket-group @(group_name) --socket-path @(s_path) --shared-dir @(dir_outside) --announce-submounts --sandbox none &
    # TODO: wait for sock and pid files to appear
    time.sleep(2)
    #lxc-usernsexec -s -m b:1000:100000:65536 -- /usr/lib/virtiofsd --socket-group flav --socket-path=/tmp/vfsd.sock --shared-dir @(d) --announce-submounts --sandbox none
    #chardev-add socket,id=char0,path=/tmp/vfsd.sock
    hypervisor.add_chardev(f"{name}-{tag}", s_path)
    hypervisor.add_virtiofs_device(1024, f"{name}-{tag}", tag)
    #hypervisor.remove_chardev(f"{name}-{tag}")
    echo pkill -9 -F @(pid_path)
    #'-device','vhost-user-fs-pci,queue-size=1024,chardev=char0,tag=myfs',
    #device_add vhost-user-fs-pci,queue-size=1024,chardev=char0,tag=myfs,bus=myroot,addr=01:00
    #mount -t virtiofs tag dir_inside
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
