#!/usr/bin/env xonsh

if __name__ == '__main__':
    d=p"$XONSH_SOURCE".resolve().parent; source @(f'{d}/bootstrap.xsh')
    MINICLUSTER.ARGPARSE.add_argument('--from', required=True, dest='from2', metavar='FROM')
    MINICLUSTER.ARGPARSE.add_argument('--to', required=True)
    MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import logging
import cluster.qmp
import cluster.functions
import shutil
import os
import base64
import hashlib


def parse_params(cwd, logger, copy_from, copy_to, additional_env={}):
    to_remote = False
    from_remote = False
    name_to = None
    name_from = None
    conn_to = None
    conn_from = None
    env = {**MINICLUSTER._asdict(), **additional_env}
    copy_from = copy_from.format(**env)
    copy_to = copy_to.format(**env)

    if ':' in copy_to:
        (name_to, copy_to) = copy_to.split(':')
        to_remote = True
        s = f"{cwd}/qga-{name_to}.sock"
        logger.debug(f"socket {s=}")
        conn_to = cluster.qmp.Connection(s, logger)
    else:
        (name_to, copy_to) = (None, copy_to)
    if ':' in copy_from:
        from_remote = True
        (name_from, copy_from) = copy_from.split(':')
        from_remote = True
        s = f"{cwd}/qga-{name_from}.sock"
        logger.debug(f"socket {s=}")
        conn_from = cluster.qmp.Connection(s, logger)
    else:
        (name_from, copy_from) = (None, copy_from)

    logger.info(f"{copy_from=} {copy_to=} {name_from=} {name_to=}")

    is_dir = False
    if from_remote:
        logger.info(f"copy from remote {copy_from=}")
        st = conn_from.path_stat(copy_from)
        if st['S_ISDIR']:
            is_dir = True
    else:
        st = cluster.functions.path_stat(copy_from, logger)
        if st['S_ISDIR']:
            is_dir = True

    return (copy_from, copy_to, name_from, name_to, from_remote, to_remote, conn_from, conn_to, is_dir)


def copy_from_local_dir_to_remote_dir(logger, cwd, copy_from, copy_to, name_from, name_to, from_remote, to_remote, conn_from, conn_to):
    logger.info(f"{copy_from=} {copy_to=} {from_remote=} {to_remote=}")
    # TODO: fix below
    base_name = os.path.basename(os.path.realpath(copy_from))
    root_dir = str(pf"{copy_from}".resolve().parent)
    base_dir = pf"{copy_from}".resolve().name
    f = shutil.make_archive(base_name, "gztar", root_dir, base_dir, logger=logger)
    logger.info(f"archive made: {f=}")
    f_size = os.stat(f).st_size
    f_basename = os.path.basename(f)
    copy_to = f"{name_to}:{copy_to}/{f_basename}"
    logger.info(f"{copy_from=} {copy_to=} {f_size=}")
    (copy_from, copy_to, name_from, name_to, from_remote, to_remote, conn_from, conn_to, is_dir) = parse_params(cwd, logger, f, copy_to)
    written = copy_from_local_file_to_remote_file(logger, cwd, copy_from, copy_to, name_from, name_to, from_remote, to_remote, conn_from, conn_to)
    assert written is not None, f"could not copy from local dir to remote dir {copy_from=} {copy_to=} {name_from=} {name_to=} {cwd=}"
    # TODO: better error handling
    conn_to.unarchive_in_vm(copy_to)
    os.remove(f)

def copy_from_local_file_to_remote_file(logger, cwd, copy_from, copy_to, name_from, name_to, from_remote, to_remote, conn_from, conn_to):
    if copy_to.endswith('/'):
        fname = str(pf"{copy_from}".name)
        copy_to = f"{copy_to}{fname}"
    logger.info(f"{copy_from=} {name_to=} {copy_to=}")
    with open(copy_from, mode='rb', buffering=0) as fp:
        written = conn_to.write_to_vm(fp, copy_to)
        logger.info(f"{written=}")
        return written
    return None

def copy_from_remote_dir_to_local_dir(logger, cwd, copy_from, copy_to, name_from, name_to, from_remote, to_remote, conn_from, conn_to):
    logger.info(f"{copy_from=} {copy_to=} {from_remote=} {to_remote=}")

def copy_from_remote_file_to_local_file(logger, cwd, copy_from, copy_to, name_from, name_to, from_remote, to_remote, conn_from, conn_to):
    st = cluster.functions.path_stat(copy_to, logger)
    if st['S_ISDIR']:
        fname = str(pf"{copy_from}".name)
        copy_to = f"{copy_to}/{fname}"
    with open(copy_to, mode='wb', buffering=0) as fp:
        read = conn_from.read_from_vm(fp, copy_from)
        logger.info(f"{read=}")
        return read
    #logger.info(f"{copy_from=} {copy_to=} {from_remote=} {to_remote=}")

def copy_from_remote_file_to_remote_file(logger, cwd, copy_from, copy_to, name_from, name_to, from_remote, to_remote, conn_from, conn_to):
    logger.info(f"{copy_from=} {copy_to=} {from_remote=} {to_remote=}")

def copy_from_remote_dir_to_remote_dir(logger, cwd, copy_from, copy_to, name_from, name_to, from_remote, to_remote, conn_from, conn_to):
    logger.info(f"{copy_from=} {copy_to=} {from_remote=} {to_remote=}")

def command_copy_files_xsh(cwd, logger, copy_from, copy_to, additional_env={}):
    # TODO: deal with single files


    (copy_from, copy_to, name_from, name_to, from_remote, to_remote, conn_from, conn_to, is_dir) = parse_params(cwd, logger, copy_from, copy_to, additional_env)
    logger.info(f"copying values: {copy_from=} {copy_to=} {name_from=} {name_to=} {from_remote=} {to_remote=} {is_dir=}")
    if from_remote:
        if to_remote:
            if is_dir:
                written = copy_from_remote_dir_to_remote_dir(logger, cwd, copy_from, copy_to, name_from, name_to, from_remote, to_remote, conn_from, conn_to)
            else:
                written = copy_from_remote_file_to_remote_file(logger, cwd, copy_from, copy_to, name_from, name_to, from_remote, to_remote, conn_from, conn_to)
        else:
            if is_dir:
                written = copy_from_remote_dir_to_local_dir(logger, cwd, copy_from, copy_to, name_from, name_to, from_remote, to_remote, conn_from, conn_to)
            else:
                written = copy_from_remote_file_to_local_file(logger, cwd, copy_from, copy_to, name_from, name_to, from_remote, to_remote, conn_from, conn_to)
    else:
        if to_remote:
            if is_dir:
                written = copy_from_local_dir_to_remote_dir(logger, cwd, copy_from, copy_to, name_from, name_to, from_remote, to_remote, conn_from, conn_to)
            else:
                written = copy_from_local_file_to_remote_file(logger, cwd, copy_from, copy_to, name_from, name_to, from_remote, to_remote, conn_from, conn_to)
        else:
            raise Exception("Cannot copy from local to local")

    return written

if __name__ == '__main__':
    cwd = MINICLUSTER.CWD_START
    logger = logging.getLogger(__name__)
    copy_from = MINICLUSTER.ARGS.from2
    copy_to = MINICLUSTER.ARGS.to
    $RAISE_SUBPROC_ERROR = True
    $XONSH_SHOW_TRACEBACK = True
    command_copy_files_xsh(cwd, logger, copy_from, copy_to)
