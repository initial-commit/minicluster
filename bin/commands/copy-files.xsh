#!/usr/bin/env xonsh

if __name__ == '__main__':
    d=p"$XONSH_SOURCE".resolve().parent; source @(f'{d}/bootstrap.xsh')
    MINICLUSTER.ARGPARSE.add_argument('--from', required=True)
    MINICLUSTER.ARGPARSE.add_argument('--to', required=True)
    MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import logging
import cluster.qmp
import shutil

def command_copy_files_xsh(cwd, logger, copy_from, copy_to):
    if ':' in copy_to:
        (name, copy_to) = copy_to.split(':')
    else:
        raise Exception("not implemented yet")
    s = f"/tmp/qga-{name}.sock"
    c = cluster.qmp.Connection(s)
    f = shutil.make_archive()
    data_from = c.write_to_vm(data_from, copy_to)

if __name__ == '__main__':
    cwd = MINICLUSTER.CWD_START
    logger = logging.getLogger(__name__)
    copy_from = MINICLUSTER.ARGS.from
    copy_to = MINICLUSTER.ARGS.to
    $RAISE_SUBPROC_ERROR = True
    command_copy_files_xsh(cwd, logger, copy_from, copy_to)
