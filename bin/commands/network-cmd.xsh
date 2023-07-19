#!/usr/bin/env xonsh

if __name__ == '__main__':
    d=p"$XONSH_SOURCE".resolve().parent; source f'{d}/bootstrap.xsh'
    MINICLUSTER.ARGPARSE.add_argument('--name', required=True)
    from cluster.functions import str2bool_exc as strtobool
    MINICLUSTER.ARGPARSE.add_argument('--network', nargs='?', type=lambda b:bool(strtobool(b)), const=False, default=True, metavar='true|false')
    MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import logging
import os
import json
import cluster.hypervisor

def command_network_cmd_xsh(cwd, logger, name, state):
    s = f"{cwd}/monitor-{name}.sock"
    conn = cluster.hypervisor.HypervisorConnection(s, logger)
    if state:
	conn.network_on()
    else:
	conn.network_off()


if __name__ == '__main__':
    cwd = MINICLUSTER.CWD_START

    logger = logging.getLogger(__name__)
    name = MINICLUSTER.ARGS.name
    network = MINICLUSTER.ARGS.network
    $XONSH_SHOW_TRACEBACK = True
    command_network_cmd_xsh(cwd, logger, name, network)
