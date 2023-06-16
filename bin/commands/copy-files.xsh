#!/usr/bin/env xonsh

if __name__ == '__main__':
    d=p"$XONSH_SOURCE".resolve().parent; source @(f'{d}/bootstrap.xsh')
    MINICLUSTER.ARGPARSE.add_argument('--from', required=True, dest='from2', metavar='FROM')
    MINICLUSTER.ARGPARSE.add_argument('--to', required=True)
    MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import logging
import cluster.qmp
import shutil
import os
import base64
import hashlib

def command_copy_files_xsh(cwd, logger, copy_from, copy_to):
    # TODO: deal with single files
    if ':' in copy_to:
        (name_to, copy_to) = copy_to.split(':')
    else:
        raise Exception("not implemented yet")
    if ':' in copy_from:
        raise Exception("not implemented yet")
    else:
        (name_from, copy_from) = (None, copy_from)
    copy_from = copy_from.format(**MINICLUSTER._asdict())
    base_name = os.path.basename(os.path.realpath(copy_from))
    root_dir = str(pf"{copy_from}".resolve().parent)
    base_dir = pf"{copy_from}".resolve().name
    s = f"{cwd}/qga-{name_to}.sock"
    c = cluster.qmp.Connection(s, logger)
    copy_to_stat = c.path_stat(copy_to)
    f = shutil.make_archive(base_name, "gztar", root_dir, base_dir, logger=logger)
    f_size = os.stat(f).st_size
    if copy_to_stat['S_ISDIR']:
        f_basename = os.path.basename(f)
        copy_to = f"{copy_to}/{f_basename}"
    with open(f, mode='rb') as fp:
        raw_data = fp.read()
        md5 = hashlib.md5(raw_data).hexdigest()
    written = c.write_to_vm(raw_data, copy_to)
    assert(f_size == written), "transfering file correctly failed"
    os.remove(f)
    return written

if __name__ == '__main__':
    cwd = MINICLUSTER.CWD_START
    logger = logging.getLogger(__name__)
    copy_from = MINICLUSTER.ARGS.from2
    copy_to = MINICLUSTER.ARGS.to
    $RAISE_SUBPROC_ERROR = True
    command_copy_files_xsh(cwd, logger, copy_from, copy_to)
