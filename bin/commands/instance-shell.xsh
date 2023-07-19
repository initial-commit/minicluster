#!/usr/bin/env xonsh

if __name__ == '__main__':
    d=p"$XONSH_SOURCE".resolve().parent; source f'{d}/bootstrap.xsh'
    MINICLUSTER.ARGPARSE.add_argument('--name', required=True)
    MINICLUSTER.ARGPARSE.add_argument('-c', '--command', required=True)
    #MINICLUSTER.ARGPARSE.add_argument('--user', default='root')
    MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

#TODO: return queues for reading commands and stdout, stderr and exit code
#TODO: mechanism to handle long-running commands, yet still continuously output

import logging
import cluster.qmp

def command_instance_shell_simple_xsh(cwd, logger, name, command, interval=0.1):
    s = f"{cwd}/qga-{name}.sock"
    conn = cluster.qmp.Connection(s, logger)
    st = conn.guest_exec_wait(command, interval=interval)
    logger.info(f"{st=}")
    return st


if __name__ == '__main__':
    cwd = MINICLUSTER.CWD_START

    logger = logging.getLogger(__name__)
    command = MINICLUSTER.ARGS.command
    name = MINICLUSTER.ARGS.name
    command_instance_shell_simple_xsh(cwd, logger, name, command)
