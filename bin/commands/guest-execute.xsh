#!/usr/bin/env xonsh

if __name__ == '__main__':
    d=p"$XONSH_SOURCE".resolve().parent; source @(f'{d}/bootstrap.xsh')
    MINICLUSTER.ARGPARSE.add_argument('--name', required=True)
    MINICLUSTER.ARGPARSE.add_argument('--cmd', required=True)
    MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import logging
import cluster.qmp
import shutil
import os
import base64
import hashlib

if __name__ == '__main__':
    cwd = MINICLUSTER.CWD_START
    logger = logging.getLogger(__name__)
    name = MINICLUSTER.ARGS.name
    cmd = MINICLUSTER.ARGS.cmd
    s = f"{cwd}/qga-{name}.sock"
    c = cluster.qmp.Connection(s, logger)
    resp = c.guest_exec_wait(cmd, env=["PWD=/tmp"])
    print(f"{resp=}")
    $RAISE_SUBPROC_ERROR = True
