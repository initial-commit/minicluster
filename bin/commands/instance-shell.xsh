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

def command_instance_shell_simple_xsh(cwd, logger, name, command, interval=0.1, env={}, show_out=False):
    s = f"{cwd}/qga-{name}.sock"
    conn = cluster.qmp.Connection(s, logger)
    env = ["{k}={v}" for k,v in env.items()]
    st = conn.guest_exec_wait(command, interval=interval, env=env)
    code = st['exitcode']
    if code == 0 and show_out:
	for line in st['err-data'].splitlines():
	    logger.error(f"NESTED {name}: {line}")
	for line in st['out-data'].splitlines():
	    logger.info(f"NESTED {name}: {line}")
    if code != 0 and not show_out:
	logger.error(f"command failed: {command=} with {code=}")
	for line in st['out-data'].splitlines():
	    logger.error(f"OUT: {line}")
	for line in st['err-data'].splitlines():
	    logger.error(f"ERR: {line}")
	logger.error(f"command failed: {command=} with {code=}")
    return (st['exitcode'] == 0, st)


if __name__ == '__main__':
    cwd = MINICLUSTER.CWD_START

    logger = logging.getLogger(__name__)
    command = MINICLUSTER.ARGS.command
    name = MINICLUSTER.ARGS.name
    $RAISE_SUBPROC_ERROR = True
    (success, st) = command_instance_shell_simple_xsh(cwd, logger, name, command)
    if not success:
	sys.exit(st['exitcode'])
